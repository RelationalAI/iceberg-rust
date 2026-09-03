// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A [`Storage`] wrapper that transparently refreshes credentials and retries once on
//! failure.
//!
//! This is fork-only functionality with no upstream equivalent: it exists to back the
//! REST catalog's vended-storage-credentials feature for tables whose credentials expire
//! while a `FileIO` built once, up front, is held and reused across many requests. It is
//! built entirely against `iceberg`'s public `Storage`/`StorageFactory` traits (see
//! `iceberg::io::storage::mod` for their contract) and `iceberg-storage-opendal`'s public
//! API — it never reaches into either crate's internals, so it stays out of the way of
//! future upstream syncs.
//!
//! Every [`Storage`] method call is retried at most once: on any error, credentials are
//! refreshed via the injected [`StorageCredentialsLoader`] and the whole backend is
//! rebuilt, then the original call is retried against the fresh backend. This is
//! deliberately coarse (call-level, not request-level) and deliberately triggers on any
//! error rather than trying to classify auth-shaped failures specifically — see the
//! [`RefreshableStorage`] docs for why.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, PROP_METADATA_LOCATION,
    PROP_TABLE_IDENT, Storage, StorageConfig, StorageCredentialsLoader, StorageFactory,
};
use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde::{Deserialize, Serialize};

/// Shared, mutable state behind a [`RefreshableStorage`]. Lives behind a single `Arc` so
/// that cloning the outer handle (as `new_input`/`new_output` must, to hand a
/// `Storage`-implementing value to [`InputFile`]/[`OutputFile`]) shares state rather than
/// forking it — a refresh performed via one clone must be visible to every other clone
/// and to every already-issued `InputFile`/`OutputFile`.
struct State {
    /// The current backend and the credential version it was built with, updated
    /// together so a reader never observes a backend paired with the wrong version.
    current: RwLock<(Arc<dyn Storage>, u64)>,
    /// Factory used to (re)build the backend from merged base + credential properties.
    /// Defaults to [`OpenDalResolvingStorageFactory`], which resolves the concrete
    /// backend per call from the path's scheme rather than once at construction time —
    /// so, unlike the old fork design, this needs no separate scheme tracking at all.
    factory: Arc<dyn StorageFactory>,
    /// Base configuration properties (endpoint, region, etc.), excluding credentials and
    /// the two internal props consumed by [`RefreshableStorageFactory::build`].
    base_props: HashMap<String, String>,
    credentials_loader: Arc<dyn StorageCredentialsLoader>,
    location: String,
    table_ident: TableIdent,
    /// Serializes calls to `credentials_loader` so concurrent failures caused by the same
    /// stale credentials trigger exactly one refresh, not a thundering herd.
    refresh_lock: tokio::sync::Mutex<()>,
}

impl State {
    fn current(&self) -> (Arc<dyn Storage>, u64) {
        let guard = self.current.read().expect("current lock poisoned");
        (guard.0.clone(), guard.1)
    }

    /// Refreshes credentials and rebuilds the backend, unless another caller already did
    /// so since `seen_version` was read (double-checked locking: cheap fast path with no
    /// lock contention when a refresh already happened, `refresh_lock` serializes the
    /// actual loader call when one is genuinely needed).
    async fn refresh(&self, seen_version: u64) -> Result<()> {
        if self.current.read().expect("current lock poisoned").1 > seen_version {
            return Ok(());
        }

        let _guard = self.refresh_lock.lock().await;

        if self.current.read().expect("current lock poisoned").1 > seen_version {
            return Ok(());
        }

        let new_creds = self
            .credentials_loader
            .load_credentials(&self.table_ident, &self.location)
            .await?;
        let mut props = self.base_props.clone();
        props.extend(new_creds.config);
        let new_backend = self.factory.build(&StorageConfig::from_props(props))?;

        let mut guard = self.current.write().expect("current lock poisoned");
        let new_version = guard.1 + 1;
        *guard = (new_backend, new_version);
        Ok(())
    }
}

/// Wraps the [`FileRead`] a backend returns from `reader()` so that *every*
/// subsequent `.read()` call gets the same refresh-and-retry-once protection as a
/// direct [`RefreshableStorage`] method call — not just the call that constructed it.
///
/// This matters because a reader is typically obtained once and then used for many
/// range reads over the lifetime of a long read (e.g. a Parquet scan reading many row
/// groups) — exactly the scenario a credential expiring mid-flight needs to survive,
/// and exactly what retrying only the initial `reader()` construction would miss.
///
/// Each `.read()` call resolves the current backend and constructs a fresh inner
/// reader from it, rather than holding one open across calls. This has no real cost:
/// every `Storage` method (including `reader()` itself) already rebuilds its
/// backend's operator from config on each call, so a `Storage::reader(path)` call is
/// already exactly this cheap per invocation — and range reads are naturally
/// idempotent, so re-resolving per call is safe. It also avoids needing a lock that
/// would otherwise serialize concurrent range reads through one shared reader.
struct RefreshableReader {
    state: Arc<State>,
    path: String,
}

#[async_trait]
impl FileRead for RefreshableReader {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        let (backend, version) = self.state.current();
        let attempt: Result<Bytes> =
            async { backend.reader(&self.path).await?.read(range.clone()).await }.await;

        match attempt {
            Ok(v) => Ok(v),
            Err(e) => {
                let path = self.path.clone();
                refresh_and_retry(&self.state, version, e, move |b| async move {
                    b.reader(&path).await?.read(range).await
                })
                .await
            }
        }
    }
}

/// A [`Storage`] that wraps another [`Storage`] (by default, any backend
/// [`iceberg-storage-opendal`](iceberg_storage_opendal) can build) and transparently
/// refreshes credentials on failure.
///
/// # Retry behavior
///
/// Every call is retried **at most once**: on any `Err`, credentials are refreshed and
/// the backend rebuilt, then the same call is retried against the fresh backend — whether
/// that retry succeeds or fails, its result is returned as-is (no further retries, no
/// backoff). This is deliberate, not a simplification of a more precise design:
///
/// - The wrapped backend's errors don't reliably distinguish "credentials are stale" from
///   any other failure. `iceberg-storage-opendal` maps every `opendal::Error` to a single
///   flat `ErrorKind::Unexpected` regardless of cause, and a non-opendal `Storage`
///   implementation isn't obligated to preserve any particular error shape at all — so
///   there is no reliable signal to key a narrower heuristic on at this layer.
/// - Retrying on any error, unconditionally, is exact parity with this crate's
///   predecessor (an opendal-accessor-level wrapper), which had no error-kind filter
///   either.
///
/// `delete_stream` is one exception: the input `Stream` is consumed by the first
/// attempt and can't be safely replayed, so it is never retried — a failure always
/// propagates immediately, matching this crate's predecessor's already-accepted lack of
/// retry for several stream-based operations.
///
/// `reader`/`writer` are handled specially, since both return a long-lived object
/// rather than a single result:
/// - `reader()`'s returned [`FileRead`] is wrapped ([`RefreshableReader`]) so every
///   subsequent `.read()` call gets this same retry-once treatment, not just the call
///   that constructed it — this crate's predecessor gave every read this protection
///   for a reader's whole lifetime (via an opendal-level accessor wrapper), and a
///   long-lived reader used for many range reads (e.g. a Parquet scan) is exactly the
///   scenario this whole mechanism exists to protect.
/// - `writer()`'s returned [`FileWrite`] is NOT wrapped this way — see its own doc
///   comment for why a stateful writer can't safely retry a failed write against a
///   freshly-constructed one.
pub struct RefreshableStorage {
    /// `None` only after deserialization, which cannot reconstruct live credential state
    /// (the credentials loader and current backend are never serializable) — every method
    /// returns an error in that case rather than panicking.
    state: Option<Arc<State>>,
}

impl Clone for RefreshableStorage {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl fmt::Debug for RefreshableStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefreshableStorage").finish()
    }
}

// Manual, always-empty Serialize/Deserialize so this can satisfy `#[typetag::serde]`'s
// requirement on `Storage` despite genuinely holding no serializable state — mirrors how
// this crate's predecessor's `Refreshable` variant `#[serde(skip)]`-ed its backend field.
impl Serialize for RefreshableStorage {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for RefreshableStorage {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        <()>::deserialize(deserializer)?;
        Ok(Self { state: None })
    }
}

impl RefreshableStorage {
    fn new(
        initial_backend: Arc<dyn Storage>,
        factory: Arc<dyn StorageFactory>,
        base_props: HashMap<String, String>,
        credentials_loader: Arc<dyn StorageCredentialsLoader>,
        location: String,
        table_ident: TableIdent,
    ) -> Self {
        Self {
            state: Some(Arc::new(State {
                current: RwLock::new((initial_backend, 0)),
                factory,
                base_props,
                credentials_loader,
                location,
                table_ident,
                refresh_lock: tokio::sync::Mutex::new(()),
            })),
        }
    }

    fn state(&self) -> Result<&Arc<State>> {
        self.state.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "RefreshableStorage: deserialized instance has no live credential state and \
                 cannot perform I/O",
            )
        })
    }
}

/// Refreshes credentials (double-checked against `seen_version`) and retries `op`
/// against the freshly-refreshed backend. If `op` also fails, `original_err` is
/// attached as context on the final error — so a caller debugging the failure can see
/// what the *first* attempt failed with too, not just whatever the retry hit, which
/// may be an unrelated, more confusing symptom (e.g. credentials refresh successfully
/// but the retry then 404s for a genuinely separate reason).
async fn refresh_and_retry<T, F, Fut>(
    state: &State,
    seen_version: u64,
    original_err: Error,
    op: F,
) -> Result<T>
where
    F: FnOnce(Arc<dyn Storage>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    state.refresh(seen_version).await?;
    let (backend, _) = state.current();
    op(backend)
        .await
        .map_err(|e| e.with_context("original_error", original_err.to_string()))
}

#[async_trait]
#[typetag::serde(name = "RefreshableStorage")]
impl Storage for RefreshableStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.exists(path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(state, version, e, |b| async move { b.exists(path).await }).await
            }
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.metadata(path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(state, version, e, |b| async move { b.metadata(path).await })
                    .await
            }
        }
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.read(path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(state, version, e, |b| async move { b.read(path).await }).await
            }
        }
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let state = Arc::clone(self.state()?);
        Ok(Box::new(RefreshableReader {
            state,
            path: path.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.write(path, bs.clone()).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(
                    state,
                    version,
                    e,
                    |b| async move { b.write(path, bs).await },
                )
                .await
            }
        }
    }

    /// Unlike `reader()`, the returned [`FileWrite`] is NOT wrapped with per-call
    /// retry — only this construction call is retried. A writer is frequently
    /// stateful (e.g. a multipart upload accumulating state across `.write()` calls),
    /// so a write that fails partway through can't be safely retried by constructing
    /// a fresh writer and replaying just the failed call: any bytes already
    /// successfully written to the old, now-abandoned writer would be silently lost
    /// rather than replayed, potentially producing truncated output. Reads don't
    /// have this problem — a range read is naturally idempotent, so retrying it
    /// against a freshly-constructed reader is always safe.
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.writer(path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(state, version, e, |b| async move { b.writer(path).await }).await
            }
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.delete(path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(state, version, e, |b| async move { b.delete(path).await }).await
            }
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let state = self.state()?;
        let (backend, version) = state.current();
        match backend.delete_prefix(path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                refresh_and_retry(
                    state,
                    version,
                    e,
                    |b| async move { b.delete_prefix(path).await },
                )
                .await
            }
        }
    }

    /// Not retried — see the [`RefreshableStorage`] docs.
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.state()?.current().0.delete_stream(paths).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.state()?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.state()?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// A [`StorageFactory`] that builds a [`RefreshableStorage`].
///
/// Inject it at catalog construction time via `with_storage_factory`. At table-load time
/// the catalog populates [`StorageConfig`] with the table identity and metadata location
/// (`FileIOBuilder::with_table_ident`/`with_location`); `build()` reads those to give the
/// credentials loader context on every refresh.
///
/// By default, credentials are refreshed against a backend built via
/// [`OpenDalResolvingStorageFactory`] (auto-detecting S3/GCS/OSS/ADLS/etc. from each
/// path), so one injected factory instance can serve tables across multiple storage
/// backends. Use [`RefreshableStorageFactory::with_factory`] to scope it to a specific,
/// pre-configured [`StorageFactory`] instead.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use iceberg::io::StorageCredentialsLoader;
/// use iceberg_storage_refreshable::RefreshableStorageFactory;
///
/// // Implement your own loader:
/// // let loader: Arc<dyn StorageCredentialsLoader> = ...;
/// // let factory = Arc::new(RefreshableStorageFactory::new(loader));
/// // catalog_config.with_storage_factory(factory);
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshableStorageFactory {
    /// `None` only after deserialization (field is skipped).
    #[serde(skip)]
    credentials_loader: Option<Arc<dyn StorageCredentialsLoader>>,
    /// `None` means "use the default `OpenDalResolvingStorageFactory`", resolved lazily at
    /// `build()` time rather than eagerly in `new()` so a plain `#[serde(skip)]` (with no
    /// `Default` requirement on `Arc<dyn StorageFactory>`) suffices here too.
    #[serde(skip)]
    inner_factory: Option<Arc<dyn StorageFactory>>,
}

impl RefreshableStorageFactory {
    /// Creates a new factory that refreshes credentials via `credentials_loader`, building
    /// backends via the default [`OpenDalResolvingStorageFactory`].
    pub fn new(credentials_loader: Arc<dyn StorageCredentialsLoader>) -> Self {
        Self {
            credentials_loader: Some(credentials_loader),
            inner_factory: None,
        }
    }

    /// Scopes this factory to build backends via `factory` instead of the default
    /// [`OpenDalResolvingStorageFactory`] — e.g. a pre-configured
    /// `iceberg_storage_opendal::OpenDalStorageFactory::S3 { .. }` for a caller that only
    /// ever serves one backend and wants to avoid per-call scheme resolution.
    pub fn with_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.inner_factory = Some(factory);
        self
    }
}

#[typetag::serde]
impl StorageFactory for RefreshableStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let loader = self.credentials_loader.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "RefreshableStorageFactory: credentials loader unavailable after deserialization",
            )
        })?;

        // Extract runtime context from props, stripping the internal keys so they don't
        // leak into the wrapped backend's own configuration.
        let mut props = config.props().clone();
        let location = props.remove(PROP_METADATA_LOCATION).unwrap_or_default();
        let table_ident = props
            .remove(PROP_TABLE_IDENT)
            .and_then(|s| serde_json::from_str::<TableIdent>(&s).ok())
            .unwrap_or_else(|| {
                TableIdent::new(
                    NamespaceIdent::new("unknown".to_string()),
                    "unknown".to_string(),
                )
            });

        let inner_factory = self
            .inner_factory
            .clone()
            .unwrap_or_else(|| Arc::new(OpenDalResolvingStorageFactory::new()));

        let initial_backend = inner_factory.build(&StorageConfig::from_props(props.clone()))?;

        Ok(Arc::new(RefreshableStorage::new(
            initial_backend,
            inner_factory,
            props,
            Arc::clone(loader),
            location,
            table_ident,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use iceberg::io::StorageCredential;

    use super::*;

    /// A minimal in-memory [`Storage`]/[`StorageFactory`] pair, independent of
    /// `iceberg-storage-opendal`, so these tests stay fast and exercise only this crate's
    /// own retry/refresh logic. `fail_until_version` lets a test simulate "this backend
    /// works once a specific credential version is loaded".
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FakeStorage {
        version: u64,
        #[serde(skip)]
        fail_until_version: u64,
    }

    #[async_trait]
    #[typetag::serde(name = "FakeStorage")]
    impl Storage for FakeStorage {
        async fn exists(&self, _path: &str) -> Result<bool> {
            if self.version < self.fail_until_version {
                Err(Error::new(ErrorKind::Unexpected, "stale credentials"))
            } else {
                Ok(true)
            }
        }
        async fn metadata(&self, _path: &str) -> Result<FileMetadata> {
            unimplemented!()
        }
        async fn read(&self, _path: &str) -> Result<Bytes> {
            unimplemented!()
        }
        async fn reader(&self, _path: &str) -> Result<Box<dyn FileRead>> {
            Ok(Box::new(FakeReader {
                version: self.version,
                fail_until_version: self.fail_until_version,
            }))
        }
        async fn write(&self, _path: &str, _bs: Bytes) -> Result<()> {
            unimplemented!()
        }
        async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
            unimplemented!()
        }
        async fn delete(&self, _path: &str) -> Result<()> {
            unimplemented!()
        }
        async fn delete_prefix(&self, _path: &str) -> Result<()> {
            unimplemented!()
        }
        async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> Result<()> {
            unimplemented!()
        }
        fn new_input(&self, _path: &str) -> Result<InputFile> {
            unimplemented!()
        }
        fn new_output(&self, _path: &str) -> Result<OutputFile> {
            unimplemented!()
        }
    }

    /// A [`FileRead`] whose `.read()` succeeds only once `version >= fail_until_version`
    /// — fixed at construction time (mirrors the version the *backend* that built it was
    /// built with), so a reader obtained before a refresh keeps failing on every `.read()`
    /// call even after a later, separately-obtained backend would succeed. This is what
    /// makes `reader_retries_individual_read_calls_not_just_construction` a meaningful
    /// test of [`RefreshableReader`] specifically, not just of `reader()`'s construction.
    struct FakeReader {
        version: u64,
        fail_until_version: u64,
    }

    #[async_trait]
    impl FileRead for FakeReader {
        async fn read(&self, _range: std::ops::Range<u64>) -> Result<Bytes> {
            if self.version < self.fail_until_version {
                Err(Error::new(ErrorKind::Unexpected, "stale credentials"))
            } else {
                Ok(Bytes::from_static(b"data"))
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FakeStorageFactory {
        fail_until_version: u64,
    }

    #[typetag::serde]
    impl StorageFactory for FakeStorageFactory {
        fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
            let version = config
                .get("version")
                .map(|v| v.parse().unwrap())
                .unwrap_or(0);
            Ok(Arc::new(FakeStorage {
                version,
                fail_until_version: self.fail_until_version,
            }))
        }
    }

    /// Returns credentials whose `config` bumps `version` by one each call, and tracks how
    /// many times it was invoked.
    struct TrackingLoader {
        call_count: AtomicUsize,
    }

    impl fmt::Debug for TrackingLoader {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TrackingLoader").finish()
        }
    }

    #[async_trait]
    impl StorageCredentialsLoader for TrackingLoader {
        async fn load_credentials(
            &self,
            _table_ident: &TableIdent,
            _location: &str,
        ) -> Result<StorageCredential> {
            let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(StorageCredential {
                prefix: String::new(),
                config: HashMap::from([("version".to_string(), n.to_string())]),
            })
        }
    }

    fn build(fail_until_version: u64, loader: Arc<TrackingLoader>) -> Arc<dyn Storage> {
        let factory = RefreshableStorageFactory::new(loader)
            .with_factory(Arc::new(FakeStorageFactory { fail_until_version }));
        factory.build(&StorageConfig::new()).unwrap()
    }

    #[tokio::test]
    async fn succeeds_first_try_without_refreshing() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        let storage = build(0, Arc::clone(&loader));

        assert!(storage.exists("x").await.unwrap());
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn refreshes_once_and_retries_on_failure() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        // Initial backend is built at version 0; the fake only succeeds once its own
        // `version` is >= 1, i.e. only after exactly one refresh.
        let storage = build(1, Arc::clone(&loader));

        assert!(storage.exists("x").await.unwrap());
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);

        // A second call against the now-fresh backend needs no further refresh.
        assert!(storage.exists("x").await.unwrap());
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn propagates_error_when_retry_also_fails() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        // Never succeeds, no matter how many times it's refreshed.
        let storage = build(u64::MAX, Arc::clone(&loader));

        assert!(storage.exists("x").await.is_err());
        // Exactly one refresh attempt per call, not an infinite loop.
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_failures_trigger_exactly_one_refresh() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        let storage = build(1, Arc::clone(&loader));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(
                async move { storage.exists("x").await.unwrap() },
            ));
        }
        for h in handles {
            assert!(h.await.unwrap());
        }

        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refresh_is_visible_through_new_input_after_construction() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        let storage = build(1, Arc::clone(&loader));

        // new_input's returned InputFile must observe the *same* shared state as the
        // storage it was created from -- not an independent snapshot -- so a refresh
        // triggered through one still succeeds when read back through the other.
        let input = storage.new_input("x").unwrap();
        assert!(input.exists().await.unwrap());
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reader_retries_individual_read_calls_not_just_construction() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        let storage = build(1, Arc::clone(&loader));

        // Obtain the reader *before* any refresh has happened -- reader() itself
        // always succeeds immediately (construction is deferred), so this must not
        // trigger a refresh on its own.
        let reader = storage.reader("x").await.unwrap();
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 0);

        // The first .read() call is what actually hits the (still-stale) backend and
        // must itself trigger exactly one refresh-and-retry -- proving retry isn't
        // limited to whatever call constructed the reader.
        let data = reader.read(0..4).await.unwrap();
        assert_eq!(data, Bytes::from_static(b"data"));
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);

        // A second .read() call on the *same*, already-obtained reader must observe
        // the already-refreshed state and need no further refresh.
        reader.read(0..4).await.unwrap();
        assert_eq!(loader.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_failure_preserves_original_error_as_context() {
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        // Never succeeds, no matter how many times it's refreshed -- so the retry
        // fails too, and its error must still mention the *original* failure.
        let storage = build(u64::MAX, Arc::clone(&loader));

        let err = storage.exists("x").await.unwrap_err();
        let rendered = format!("{err:?}");
        assert!(
            rendered.contains("original_error"),
            "error should carry the original attempt's failure as context: {rendered}"
        );
        assert!(
            rendered.contains("stale credentials"),
            "context should contain the original error's message: {rendered}"
        );
    }

    #[tokio::test]
    async fn write_and_read_roundtrip_through_real_backend() {
        // Unlike the other tests here, this goes through a real
        // iceberg-storage-opendal backend (in-memory) rather than FakeStorage, to
        // prove write()/read() actually work end-to-end through this wrapper, not
        // just that the retry bookkeeping around them is correct.
        let loader = Arc::new(TrackingLoader {
            call_count: AtomicUsize::new(0),
        });
        let factory = RefreshableStorageFactory::new(loader).with_factory(Arc::new(
            iceberg_storage_opendal::OpenDalStorageFactory::Memory,
        ));
        let storage = factory.build(&StorageConfig::new()).unwrap();

        storage
            .write("memory:/roundtrip-file", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let data = storage.read("memory:/roundtrip-file").await.unwrap();
        assert_eq!(data, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn factory_build_parses_table_ident_and_location_from_props() {
        #[derive(Debug)]
        struct CapturingLoader {
            captured: std::sync::Mutex<Option<(TableIdent, String)>>,
        }

        #[async_trait]
        impl StorageCredentialsLoader for CapturingLoader {
            async fn load_credentials(
                &self,
                table_ident: &TableIdent,
                location: &str,
            ) -> Result<StorageCredential> {
                *self.captured.lock().unwrap() = Some((table_ident.clone(), location.to_string()));
                Ok(StorageCredential {
                    prefix: String::new(),
                    config: HashMap::from([("version".to_string(), "1".to_string())]),
                })
            }
        }

        let loader = Arc::new(CapturingLoader {
            captured: std::sync::Mutex::new(None),
        });
        let table_ident = TableIdent::from_strs(["ns", "tbl"]).unwrap();
        // This is exactly what FileIOBuilder::with_table_ident/with_location populate.
        let config = StorageConfig::new()
            .with_prop(
                PROP_METADATA_LOCATION,
                "s3://test-bucket/table/metadata.json",
            )
            .with_prop(
                PROP_TABLE_IDENT,
                serde_json::to_string(&table_ident).unwrap(),
            );

        let factory = RefreshableStorageFactory::new(Arc::clone(&loader) as _).with_factory(
            Arc::new(FakeStorageFactory {
                fail_until_version: 1,
            }),
        );
        let storage = factory.build(&config).unwrap();

        // fail_until_version: 1 guarantees the first call fails and triggers exactly
        // one refresh, so the loader is actually invoked by the time we assert below.
        storage.exists("x").await.unwrap();

        let (captured_ident, captured_location) = loader.captured.lock().unwrap().clone().unwrap();
        assert_eq!(captured_ident, table_ident);
        assert_eq!(captured_location, "s3://test-bucket/table/metadata.json");
    }
}

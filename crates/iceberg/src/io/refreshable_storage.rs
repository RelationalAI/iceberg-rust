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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use opendal::Operator;
use opendal::raw::*;

use super::opendal::OpenDalStorage;
use super::refreshable_accessor::RefreshableAccessor;
use crate::io::file_io::Extensions;
use crate::io::{StorageCredential, StorageCredentialsLoader};
use crate::{Error, ErrorKind, Result};

/// Holds shared configuration and state for credential refresh.
///
/// Multiple `RefreshableAccessor` instances share a single `RefreshableOpenDalStorage`
/// via `Arc`, allowing credential refreshes to be visible across all accessors.
pub struct RefreshableOpenDalStorage {
    /// Scheme of the inner backend (e.g., "s3", "azdls")
    scheme: String,

    /// Base properties (non-credential config like endpoint, region, etc.)
    base_props: HashMap<String, String>,

    /// Inner storage (built in new, rebuilt on credential refresh)
    pub(crate) inner_storage: Mutex<OpenDalStorage>,

    /// Credential loader
    credentials_loader: Arc<dyn StorageCredentialsLoader>,

    /// Extensions for building storage (e.g. custom S3 credential loaders)
    extensions: Extensions,

    /// Current credentials from last refresh (shared across accessors)
    current_credentials: Mutex<Option<StorageCredential>>,

    /// Cached AccessorInfo (created lazily from first operator)
    pub(crate) cached_info: Mutex<Option<Arc<AccessorInfo>>>,

    /// Monotonically increasing version number, incremented each time credentials
    /// are refreshed via do_refresh. Used by RefreshableAccessor instances to detect
    /// whether someone else has already refreshed since their accessor was built.
    credential_version: AtomicU64,

    /// Async mutex that serializes calls to the external credential loader.
    /// Held across the await point of maybe_load_credentials to ensure only
    /// one concurrent caller invokes the loader at a time.
    refresh_lock: AsyncMutex<()>,
}

impl std::fmt::Debug for RefreshableOpenDalStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableOpenDalStorage").finish()
    }
}

impl RefreshableOpenDalStorage {
    /// Creates a new RefreshableOpenDalStorage.
    ///
    /// # Arguments
    /// * `scheme` - Storage scheme (e.g., "s3", "azdls")
    /// * `base_props` - Base configuration properties (without credentials)
    /// * `credentials_loader` - Loader for refreshing credentials
    /// * `initial_credentials` - Initial credentials (if any), stored as current_credentials
    /// * `extensions` - Extensions for building storage
    pub fn new(
        scheme: String,
        base_props: HashMap<String, String>,
        credentials_loader: Arc<dyn StorageCredentialsLoader>,
        initial_credentials: Option<StorageCredential>,
        extensions: Extensions,
    ) -> Result<Self> {
        // Build initial inner_storage from base_props + initial_credentials
        let mut props = base_props.clone();
        if let Some(ref creds) = initial_credentials {
            props.extend(creds.config.clone());
        }
        let inner_storage = OpenDalStorage::build_from_props(&scheme, props, &extensions)?;

        Ok(Self {
            scheme,
            base_props,
            inner_storage: Mutex::new(inner_storage),
            credentials_loader,
            extensions,
            current_credentials: Mutex::new(initial_credentials),
            cached_info: Mutex::new(None),
            credential_version: AtomicU64::new(0),
            refresh_lock: AsyncMutex::new(()),
        })
    }

    /// Create an operator for the given path.
    ///
    /// Builds a `RefreshableAccessor` that wraps the inner storage operator and
    /// delegates all operations through credential refresh logic.
    pub fn refreshable_create_operator(self: &Arc<Self>, path: &str) -> Result<(Operator, String)> {
        let storage_guard = self.inner_storage.lock().unwrap();
        let path_string = path.to_string();
        let (operator, relative_path) = storage_guard.create_operator(&path_string)?;
        let relative_path = relative_path.to_string();
        drop(storage_guard);

        let accessor = operator.into_inner();

        // Cache AccessorInfo if not already cached
        {
            let mut info_guard = self.cached_info.lock().unwrap();
            if info_guard.is_none() {
                *info_guard = Some(accessor.info());
            }
        }

        let version = self.credential_version();
        let refreshable_accessor =
            RefreshableAccessor::new(accessor, version, path.to_string(), Arc::clone(self));

        let wrapped_operator = Operator::from_inner(Arc::new(refreshable_accessor));
        Ok((wrapped_operator, relative_path))
    }

    /// Load credentials if available, and refresh inner storage if new ones are returned.
    ///
    /// Returns `Ok(true)` if credentials were refreshed, `Ok(false)` otherwise.
    #[cfg(test)]
    pub(crate) async fn maybe_refresh(&self) -> Result<bool> {
        // Get existing credentials (without holding lock across await)
        let existing_creds = {
            let creds_guard = self.current_credentials.lock().unwrap();
            creds_guard.clone()
        };

        let new_creds = self
            .credentials_loader
            .maybe_load_credentials("", existing_creds.as_ref())
            .await?;

        if let Some(new_creds) = new_creds {
            self.do_refresh(new_creds)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Rebuild inner storage from new credentials and bump the credential version.
    fn do_refresh(&self, new_creds: StorageCredential) -> Result<()> {
        let mut full_props = self.base_props.clone();
        full_props.extend(new_creds.config.clone());

        let new_storage =
            OpenDalStorage::build_from_props(&self.scheme, full_props, &self.extensions)?;

        *self.inner_storage.lock().unwrap() = new_storage;
        *self.current_credentials.lock().unwrap() = Some(new_creds);
        self.credential_version.fetch_add(1, Ordering::Release);

        Ok(())
    }

    /// Returns the current credential version number.
    pub(crate) fn credential_version(&self) -> u64 {
        self.credential_version.load(Ordering::Acquire)
    }

    /// Refresh credentials in response to a PermissionDenied error.
    ///
    /// Uses double-checked locking with a version number:
    /// 1. If `credential_version > accessor_version`, someone already refreshed —
    ///    return `(true, current_version)` without calling the loader.
    /// 2. Acquire `refresh_lock` (async, serializes loader calls).
    /// 3. Double-check: if `credential_version > accessor_version`, another caller
    ///    already refreshed while we waited — return `(true, current_version)`.
    /// 4. Call the loader. If new credentials, call `do_refresh`.
    /// 5. Return `(refreshed, current_version)`.
    pub(crate) async fn refresh_on_permission_denied(
        &self,
        accessor_version: u64,
    ) -> Result<(bool, u64)> {
        // Fast path: someone already refreshed since this accessor was built
        let current = self.credential_version.load(Ordering::Acquire);
        if current > accessor_version {
            return Ok((true, current));
        }

        // Acquire the async lock to serialize loader calls
        let _guard = self.refresh_lock.lock().await;

        // Double-check after acquiring lock
        let current = self.credential_version.load(Ordering::Acquire);
        if current > accessor_version {
            return Ok((true, current));
        }

        // We are the one who should call the loader
        let existing_creds = {
            let creds_guard = self.current_credentials.lock().unwrap();
            creds_guard.clone()
        };

        let new_creds = self
            .credentials_loader
            .maybe_load_credentials("", existing_creds.as_ref())
            .await?;

        if let Some(new_creds) = new_creds {
            self.do_refresh(new_creds)?;
            let new_version = self.credential_version.load(Ordering::Acquire);
            Ok((true, new_version))
        } else {
            let current = self.credential_version.load(Ordering::Acquire);
            Ok((false, current))
        }
    }
}

/// Builder for RefreshableOpenDalStorage
#[derive(Default, Debug)]
pub struct RefreshableOpenDalStorageBuilder {
    scheme: Option<String>,
    base_props: HashMap<String, String>,
    credentials_loader: Option<Arc<dyn StorageCredentialsLoader>>,
    initial_credentials: Option<StorageCredential>,
    extensions: Extensions,
}

impl RefreshableOpenDalStorageBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the storage scheme (e.g., "s3", "azdls")
    pub fn scheme(mut self, scheme: String) -> Self {
        self.scheme = Some(scheme);
        self
    }

    /// Set the base properties (non-credential configuration)
    pub fn base_props(mut self, props: HashMap<String, String>) -> Self {
        self.base_props = props;
        self
    }

    /// Set the credentials loader
    pub fn credentials_loader(mut self, loader: Arc<dyn StorageCredentialsLoader>) -> Self {
        self.credentials_loader = Some(loader);
        self
    }

    /// Set the initial credentials (if any)
    pub fn initial_credentials(mut self, creds: Option<StorageCredential>) -> Self {
        self.initial_credentials = creds;
        self
    }

    /// Set the extensions
    pub fn extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Build the RefreshableOpenDalStorage wrapped in Arc
    pub fn build(self) -> Result<Arc<RefreshableOpenDalStorage>> {
        Ok(Arc::new(RefreshableOpenDalStorage::new(
            self.scheme
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "scheme is required"))?,
            self.base_props,
            self.credentials_loader.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "credentials_loader is required")
            })?,
            self.initial_credentials,
            self.extensions,
        )?))
    }
}

#[cfg(all(test, feature = "storage-memory"))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::io::StorageCredential;

    // --- Test helpers ---

    /// Always returns `None` — simulates "credentials are still valid, no refresh needed".
    #[derive(Debug)]
    struct NeverRefreshLoader;

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for NeverRefreshLoader {
        async fn maybe_load_credentials(
            &self,
            _location: &str,
            _existing_credentials: Option<&StorageCredential>,
        ) -> Result<Option<StorageCredential>> {
            Ok(None)
        }
    }

    /// Always returns `Some(StorageCredential)` — simulates "always provide fresh credentials".
    #[derive(Debug)]
    struct AlwaysRefreshLoader;

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for AlwaysRefreshLoader {
        async fn maybe_load_credentials(
            &self,
            _location: &str,
            _existing_credentials: Option<&StorageCredential>,
        ) -> Result<Option<StorageCredential>> {
            Ok(Some(StorageCredential {
                prefix: "memory:/refreshed/".to_string(),
                config: HashMap::from([("refreshed_key".to_string(), "refreshed_val".to_string())]),
            }))
        }
    }

    /// Records every call: increments a counter and stores the `existing_credentials` argument.
    /// Returns `Some` with a credential whose `config` contains the call number (e.g. `{"call": "1"}`).
    /// This lets tests assert what credentials were passed to the loader and in what order.
    struct TrackingRefreshLoader {
        call_count: AtomicUsize,
        received_existing: Mutex<Vec<Option<StorageCredential>>>,
    }

    impl std::fmt::Debug for TrackingRefreshLoader {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TrackingRefreshLoader").finish()
        }
    }

    impl TrackingRefreshLoader {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                received_existing: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }

        fn received_existing(&self) -> Vec<Option<StorageCredential>> {
            self.received_existing.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for TrackingRefreshLoader {
        async fn maybe_load_credentials(
            &self,
            _location: &str,
            existing_credentials: Option<&StorageCredential>,
        ) -> Result<Option<StorageCredential>> {
            let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
            self.received_existing
                .lock()
                .unwrap()
                .push(existing_credentials.cloned());

            Ok(Some(StorageCredential {
                prefix: format!("memory:/refresh-{n}/"),
                config: HashMap::from([("call".to_string(), n.to_string())]),
            }))
        }
    }

    fn build_memory_refreshable(
        loader: Arc<dyn StorageCredentialsLoader>,
    ) -> Arc<RefreshableOpenDalStorage> {
        RefreshableOpenDalStorageBuilder::new()
            .scheme("memory".to_string())
            .base_props(HashMap::new())
            .credentials_loader(loader)
            .build()
            .expect("Failed to build RefreshableOpenDalStorage for memory")
    }

    fn build_memory_refreshable_with_initial_creds(
        loader: Arc<dyn StorageCredentialsLoader>,
        initial_creds: StorageCredential,
    ) -> Arc<RefreshableOpenDalStorage> {
        RefreshableOpenDalStorageBuilder::new()
            .scheme("memory".to_string())
            .base_props(HashMap::new())
            .credentials_loader(loader)
            .initial_credentials(Some(initial_creds))
            .build()
            .expect("Failed to build RefreshableOpenDalStorage for memory")
    }

    // --- Tests ---

    /// Verifies the basic contract: when the loader provides new credentials,
    /// `maybe_refresh` returns `Ok(true)`.
    #[tokio::test]
    async fn test_maybe_refresh_returns_true_when_new_credentials_loaded() {
        let storage = build_memory_refreshable(Arc::new(AlwaysRefreshLoader));

        let refreshed = storage.maybe_refresh().await.unwrap();
        assert!(refreshed, "Expected maybe_refresh to return true");
    }

    /// Verifies the inverse: when the loader returns `None`, no refresh happens
    /// and `maybe_refresh` returns `Ok(false)`.
    #[tokio::test]
    async fn test_maybe_refresh_returns_false_when_no_new_credentials() {
        let storage = build_memory_refreshable(Arc::new(NeverRefreshLoader));

        let refreshed = storage.maybe_refresh().await.unwrap();
        assert!(!refreshed, "Expected maybe_refresh to return false");
    }

    /// The loader needs access to current credentials to decide whether to refresh
    /// (e.g. check expiry). This test ensures the initial credentials set during
    /// construction are correctly forwarded to the loader's `existing_credentials` param.
    #[tokio::test]
    async fn test_maybe_refresh_passes_existing_credentials_to_loader() {
        let initial_creds = StorageCredential {
            prefix: "memory:/initial/".to_string(),
            config: HashMap::from([("init_key".to_string(), "init_val".to_string())]),
        };

        let loader = Arc::new(TrackingRefreshLoader::new());
        let storage = build_memory_refreshable_with_initial_creds(
            Arc::clone(&loader) as _,
            initial_creds.clone(),
        );

        // First call should receive the initial credentials
        storage.maybe_refresh().await.unwrap();

        let received = loader.received_existing();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], Some(initial_creds));
    }

    /// After `do_refresh` stores new credentials in `current_credentials`, subsequent
    /// loader calls must receive those updated credentials, not stale ones.
    /// Critical for loaders that check credential expiry.
    #[tokio::test]
    async fn test_do_refresh_updates_current_credentials() {
        let loader = Arc::new(TrackingRefreshLoader::new());
        let storage = build_memory_refreshable(Arc::clone(&loader) as _);

        // First refresh: no existing credentials passed (none were set initially)
        storage.maybe_refresh().await.unwrap();
        // Second refresh: should receive the credentials from first refresh
        storage.maybe_refresh().await.unwrap();

        let received = loader.received_existing();
        assert_eq!(received.len(), 2);

        // First call: no existing credentials
        assert_eq!(received[0], None);

        // Second call: should have the credentials produced by the first refresh
        let first_refresh_creds = StorageCredential {
            prefix: "memory:/refresh-1/".to_string(),
            config: HashMap::from([("call".to_string(), "1".to_string())]),
        };
        assert_eq!(received[1], Some(first_refresh_creds));
    }

    /// Verifies two things about `do_refresh`:
    /// 1. It actually rebuilds `inner_storage` with a fresh instance (data isolation —
    ///    data written to the old storage is gone after refresh).
    /// 2. The new credentials end up stored in `current_credentials` (the second
    ///    `maybe_refresh` call receives credentials from the first refresh, not old ones).
    #[tokio::test]
    async fn test_do_refresh_rebuilds_inner_storage() {
        let loader = Arc::new(TrackingRefreshLoader::new());
        let storage = build_memory_refreshable(Arc::clone(&loader) as _);

        // Write data via the current inner storage
        let path = "memory:/test-file".to_string();
        {
            let inner = storage.inner_storage.lock().unwrap();
            let (op, rel) = inner.create_operator(&path).unwrap();
            drop(inner);

            op.write(rel, bytes::Bytes::from("hello")).await.unwrap();

            // Verify the data is there
            let inner = storage.inner_storage.lock().unwrap();
            let (op2, rel2) = inner.create_operator(&path).unwrap();
            drop(inner);
            let data = op2.read(rel2).await.unwrap().to_bytes();
            assert_eq!(data, bytes::Bytes::from("hello"));
        }

        // Refresh credentials — this rebuilds inner_storage with a fresh memory backend
        let refreshed = storage.maybe_refresh().await.unwrap();
        assert!(refreshed);

        // The new inner storage is a fresh memory instance; old data should be gone
        let inner = storage.inner_storage.lock().unwrap();
        let (op3, rel3) = inner.create_operator(&path).unwrap();
        drop(inner);
        let exists = op3.exists(rel3).await.unwrap();
        assert!(
            !exists,
            "Data from old storage should not exist after rebuild"
        );

        // Verify the new credentials are tracked: second maybe_refresh should
        // receive the credentials from the first refresh (call=1)
        storage.maybe_refresh().await.unwrap();
        let received = loader.received_existing();
        assert_eq!(loader.call_count(), 2);
        let second_existing = received[1].as_ref().unwrap();
        assert_eq!(
            second_existing.config.get("call"),
            Some(&"1".to_string()),
            "Second refresh should receive creds from first refresh"
        );
    }

    /// Verifies that `credential_version` increments on each `do_refresh` (via `maybe_refresh`).
    #[tokio::test]
    async fn test_credential_version_increments_on_refresh() {
        let storage = build_memory_refreshable(Arc::new(AlwaysRefreshLoader));

        assert_eq!(storage.credential_version(), 0);

        storage.maybe_refresh().await.unwrap();
        assert_eq!(storage.credential_version(), 1);

        storage.maybe_refresh().await.unwrap();
        assert_eq!(storage.credential_version(), 2);
    }

    /// End-to-end sanity check that `refreshable_create_operator` produces a working
    /// `Operator` that wraps the inner storage correctly via `RefreshableAccessor`.
    #[tokio::test]
    async fn test_refreshable_operator_can_write_and_read() {
        let storage = build_memory_refreshable(Arc::new(NeverRefreshLoader));

        let (op, rel) = storage
            .refreshable_create_operator("memory:/roundtrip-file")
            .unwrap();

        op.write(&rel, bytes::Bytes::from("roundtrip data"))
            .await
            .unwrap();

        let read_back = op.read(&rel).await.unwrap().to_bytes();
        assert_eq!(read_back, bytes::Bytes::from("roundtrip data"));
    }
}

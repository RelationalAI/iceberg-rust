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

    /// Load credentials and refresh inner storage.
    #[cfg(test)]
    pub(crate) async fn refresh(&self) -> Result<()> {
        let new_creds = self.credentials_loader.load_credentials("").await?;
        self.do_refresh(new_creds)
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
    ///    return the current version without calling the loader.
    /// 2. Acquire `refresh_lock` (async, serializes loader calls).
    /// 3. Double-check: if `credential_version > accessor_version`, another caller
    ///    already refreshed while we waited — return the current version.
    /// 4. Call the loader, then `do_refresh`.
    /// 5. Return the new version.
    pub(crate) async fn refresh_on_permission_denied(
        &self,
        accessor_version: u64,
    ) -> Result<u64> {
        // Fast path: someone already refreshed since this accessor was built
        let current = self.credential_version.load(Ordering::Acquire);
        if current > accessor_version {
            return Ok(current);
        }

        // Acquire the async lock to serialize loader calls
        let _guard = self.refresh_lock.lock().await;

        // Double-check after acquiring lock
        let current = self.credential_version.load(Ordering::Acquire);
        if current > accessor_version {
            return Ok(current);
        }

        // We are the one who should call the loader
        let new_creds = self.credentials_loader.load_credentials("").await?;
        self.do_refresh(new_creds)?;
        Ok(self.credential_version.load(Ordering::Acquire))
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

    /// Simple loader that always returns the same credential.
    #[derive(Debug)]
    struct SimpleLoader;

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for SimpleLoader {
        async fn load_credentials(
            &self,
            _location: &str,
        ) -> Result<StorageCredential> {
            Ok(StorageCredential {
                prefix: "memory:/refreshed/".to_string(),
                config: HashMap::from([("refreshed_key".to_string(), "refreshed_val".to_string())]),
            })
        }
    }

    /// Records every call: increments a counter.
    /// Returns a credential whose `config` contains the call number (e.g. `{"call": "1"}`).
    struct TrackingRefreshLoader {
        call_count: AtomicUsize,
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
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for TrackingRefreshLoader {
        async fn load_credentials(
            &self,
            _location: &str,
        ) -> Result<StorageCredential> {
            let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(StorageCredential {
                prefix: format!("memory:/refresh-{n}/"),
                config: HashMap::from([("call".to_string(), n.to_string())]),
            })
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

    // --- Tests ---

    /// Verifies `refresh` calls the loader and rebuilds inner storage.
    #[tokio::test]
    async fn test_refresh_calls_loader() {
        let loader = Arc::new(TrackingRefreshLoader::new());
        let storage = build_memory_refreshable(Arc::clone(&loader) as _);

        storage.refresh().await.unwrap();
        assert_eq!(loader.call_count(), 1);

        storage.refresh().await.unwrap();
        assert_eq!(loader.call_count(), 2);
    }

    /// Verifies that `do_refresh` rebuilds `inner_storage` with a fresh instance
    /// (data written to the old storage is gone after refresh).
    #[tokio::test]
    async fn test_do_refresh_rebuilds_inner_storage() {
        let storage = build_memory_refreshable(Arc::new(SimpleLoader));

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
        storage.refresh().await.unwrap();

        // The new inner storage is a fresh memory instance; old data should be gone
        let inner = storage.inner_storage.lock().unwrap();
        let (op3, rel3) = inner.create_operator(&path).unwrap();
        drop(inner);
        let exists = op3.exists(rel3).await.unwrap();
        assert!(
            !exists,
            "Data from old storage should not exist after rebuild"
        );
    }

    /// Verifies that `credential_version` increments on each `do_refresh` (via `refresh`).
    #[tokio::test]
    async fn test_credential_version_increments_on_refresh() {
        let storage = build_memory_refreshable(Arc::new(SimpleLoader));

        assert_eq!(storage.credential_version(), 0);

        storage.refresh().await.unwrap();
        assert_eq!(storage.credential_version(), 1);

        storage.refresh().await.unwrap();
        assert_eq!(storage.credential_version(), 2);
    }

    /// End-to-end sanity check that `refreshable_create_operator` produces a working
    /// `Operator` that wraps the inner storage correctly via `RefreshableAccessor`.
    #[tokio::test]
    async fn test_refreshable_operator_can_write_and_read() {
        let storage = build_memory_refreshable(Arc::new(SimpleLoader));

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

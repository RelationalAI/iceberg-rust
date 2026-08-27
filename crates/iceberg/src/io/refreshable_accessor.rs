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

use std::future::Future;
use std::sync::{Arc, Mutex};

use opendal::raw::*;
use opendal::{Capability, OperationContext};

use super::refreshable_storage::RefreshableOpenDalStorage;
use crate::Result;

/// An OpenDAL accessor that wraps another accessor and retries after refreshing
/// credentials when any operation fails.
///
/// Each instance has its own inner accessor and shares credential state with
/// other accessors via `Arc<RefreshableOpenDalStorage>`. Credentials are only
/// refreshed when an operation fails, not proactively.
///
/// Concurrency: if multiple accessors hit errors simultaneously, only one will
/// call the external credential loader (via double-checked locking on
/// `RefreshableOpenDalStorage::try_refresh_credentials`). The others will detect
/// the version bump and simply rebuild their accessor from the already-refreshed
/// credentials.
///
/// # Known limitation (opendal 0.58 `Service` trait)
/// `read`/`write`/`list`/`delete`/`copy` are synchronous constructors in opendal's
/// `Service` trait (they build a reader/writer/etc. without performing I/O); the
/// actual I/O, and therefore any credential-expiry failure, happens later via the
/// returned `oio::Read`/`oio::Write`/etc. object, which this wrapper does not
/// control. So credential-refresh-and-retry only covers `stat`/`create_dir`/
/// `rename`/`presign` (still async on the trait) below; a long-lived reader/writer
/// held across a credential expiry window will surface the raw error instead of
/// transparently retrying. Making read/write retry-capable would require a
/// retry-aware `oio::Read`/`oio::Write` wrapper, which is not implemented here.
pub(crate) struct RefreshableAccessor {
    /// The current backend's accessor paired with the credential version it was built from.
    inner: Mutex<(Servicer, u64)>,

    /// The full original path (e.g. "memory:/some-file") used to create the operator.
    /// Needed to rebuild the accessor after credential refresh.
    original_path: String,

    /// Shared storage holding credentials and configuration
    storage: Arc<RefreshableOpenDalStorage>,
}

impl RefreshableAccessor {
    pub(crate) fn new(
        accessor: Servicer,
        credential_version: u64,
        original_path: String,
        storage: Arc<RefreshableOpenDalStorage>,
    ) -> Self {
        Self {
            inner: Mutex::new((accessor, credential_version)),
            original_path,
            storage,
        }
    }

    /// Get the current inner accessor and its credential version.
    fn get_accessor(&self) -> (Servicer, u64) {
        let guard = self.inner.lock().unwrap();
        guard.clone()
    }

    /// Rebuild the inner accessor from the shared storage after a credential refresh.
    ///
    /// Uses `original_path` (the full path passed to `refreshable_create_operator`)
    /// to call `create_operator` on the refreshed `inner_storage`.
    fn rebuild_accessor(&self, new_version: u64) -> Result<Servicer> {
        let storage_guard = self.storage.lock_inner_storage();
        let (operator, _) = storage_guard.create_operator(&self.original_path)?;
        drop(storage_guard);

        let (_ctx, new_accessor) = operator.into_parts();
        *self.inner.lock().unwrap() = (new_accessor.clone(), new_version);
        Ok(new_accessor)
    }

    /// Run an operation with automatic retry after credential refresh on any error.
    ///
    /// 1. Gets the current accessor and runs the operation.
    /// 2. If it fails (any error), calls `try_refresh_credentials` with the
    ///    accessor's credential version.
    /// 3. If credentials were refreshed (by us or another concurrent accessor),
    ///    rebuilds our accessor and retries the operation once.
    /// 4. If the retry also fails, returns an error that preserves both the
    ///    original and retry error messages.
    async fn with_credential_retry<F, Fut, T>(&self, op: F) -> opendal::Result<T>
    where
        F: Fn(Servicer) -> Fut,
        Fut: Future<Output = opendal::Result<T>>,
    {
        let (accessor, version) = self.get_accessor();
        let result = op(accessor).await;

        match result {
            Err(original_err) => {
                let original_display = original_err.to_string();
                let original_kind = original_err.kind();

                let new_version = self
                    .storage
                    .try_refresh_credentials(version)
                    .await
                    .map_err(|e| {
                        opendal::Error::new(
                            original_kind,
                            format!(
                                "Operation failed and credential refresh also failed: \
                                 {e}. Original error: {original_display}"
                            ),
                        )
                    })?;

                let new_accessor = self.rebuild_accessor(new_version).map_err(|e| {
                    opendal::Error::new(
                        opendal::ErrorKind::Unexpected,
                        format!(
                            "Failed to rebuild accessor after credential refresh. \
                             Original error: {original_display}"
                        ),
                    )
                    .set_source(e)
                })?;

                op(new_accessor).await.map_err(|retry_err| {
                    opendal::Error::new(
                        retry_err.kind(),
                        format!(
                            "Retry after credential refresh also failed. \
                             Original error: {original_display}"
                        ),
                    )
                    .set_source(retry_err)
                })
            }
            other => other,
        }
    }
}

impl std::fmt::Debug for RefreshableAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableAccessor").finish()
    }
}

impl Service for RefreshableAccessor {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        let info_guard = self.storage.lock_cached_info();
        if let Some(info) = info_guard.as_ref() {
            info.clone()
        } else {
            drop(info_guard);
            ServiceInfo::with_scheme("")
        }
    }

    fn capability(&self) -> Capability {
        let (accessor, _) = self.get_accessor();
        accessor.capability()
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.stat(ctx, path, args).await }
        })
        .await
    }

    // `read`/`write`/`list`/`delete`/`copy` are synchronous constructors in opendal's
    // `Service` trait (see the "Known limitation" doc comment above): they build a
    // reader/writer/etc. without performing I/O, so there is no failure here to retry
    // against. Delegate directly to the current accessor.
    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        let (accessor, _) = self.get_accessor();
        accessor.read(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        let (accessor, _) = self.get_accessor();
        accessor.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        let (accessor, _) = self.get_accessor();
        accessor.delete(ctx)
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        let (accessor, _) = self.get_accessor();
        accessor.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        let (accessor, _) = self.get_accessor();
        accessor.copy(ctx, from, to, args, opts)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.create_dir(ctx, path, args).await }
        })
        .await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.rename(ctx, from, to, args).await }
        })
        .await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.with_credential_retry(|accessor| {
            let args = args.clone();
            async move { accessor.presign(ctx, path, args).await }
        })
        .await
    }
}

/// Tests for the `with_credential_retry` logic in `RefreshableAccessor`.
///
/// `with_credential_retry` works as follows:
/// 1. Gets the current accessor (no refresh) and runs the operation.
/// 2. On any error, calls `try_refresh_credentials` with the accessor's
///    credential version.
/// 3. If credentials were refreshed, rebuilds the accessor and retries once.
/// 4. If the retry also fails, returns an error preserving both original and
///    retry error messages.
///
/// To test this, we inject a `FailingAccessor` (returns a configurable error on `stat`)
/// as the initial inner accessor, while the shared storage's `inner_storage` is a real
/// memory backend. When credential refresh triggers a rebuild, the accessor switches
/// from `FailingAccessor` to the real memory backend — observable as a change in error
/// kind (e.g. `PermissionDenied` → `NotFound`).
///
/// A `SequenceLoader` controls exactly which loader calls trigger refresh (`Some`) and
/// which don't (`None`), so we can test each branch of the retry logic.
#[cfg(all(test, feature = "storage-memory"))]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::NamespaceIdent;
    use crate::catalog::TableIdent;
    use crate::io::refreshable_storage::RefreshableOpenDalStorageBuilder;
    use crate::io::{StorageCredential, StorageCredentialsLoader};

    // --- Test helpers ---

    /// Returns pre-configured credentials in order from a `VecDeque`. Tracks call count.
    struct SequenceLoader {
        responses: Mutex<VecDeque<StorageCredential>>,
        call_count: AtomicUsize,
    }

    impl std::fmt::Debug for SequenceLoader {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SequenceLoader").finish()
        }
    }

    impl SequenceLoader {
        fn new(responses: Vec<StorageCredential>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                call_count: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for SequenceLoader {
        async fn load_credentials(
            &self,
            _table_ident: &TableIdent,
            _location: &str,
        ) -> Result<StorageCredential> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let mut responses = self.responses.lock().unwrap();
            Ok(responses.pop_front().unwrap_or_else(dummy_credential))
        }
    }

    /// `Service` impl that always returns a configurable `opendal::ErrorKind` on `stat`.
    /// All other methods return `Unexpected` (not expected to be called by these tests).
    struct FailingAccessor {
        error_kind: opendal::ErrorKind,
        info: ServiceInfo,
    }

    impl std::fmt::Debug for FailingAccessor {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FailingAccessor").finish()
        }
    }

    impl FailingAccessor {
        fn new(error_kind: opendal::ErrorKind, info: ServiceInfo) -> Self {
            Self { error_kind, info }
        }

        fn unexpected<T>() -> opendal::Result<T> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not implemented in test",
            ))
        }
    }

    impl Service for FailingAccessor {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;

        fn info(&self) -> ServiceInfo {
            self.info.clone()
        }

        fn capability(&self) -> Capability {
            Capability::default()
        }

        async fn stat(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpStat,
        ) -> opendal::Result<RpStat> {
            Err(opendal::Error::new(self.error_kind, "test error"))
        }

        fn read(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            Self::unexpected()
        }

        fn write(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            Self::unexpected()
        }

        fn delete(&self, _ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
            Self::unexpected()
        }

        fn list(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpList,
        ) -> opendal::Result<Self::Lister> {
            Self::unexpected()
        }

        fn copy(
            &self,
            _ctx: &OperationContext,
            _from: &str,
            _to: &str,
            _args: OpCopy,
            _opts: OpCopier,
        ) -> opendal::Result<Self::Copier> {
            Self::unexpected()
        }

        async fn create_dir(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            Self::unexpected()
        }

        async fn rename(
            &self,
            _ctx: &OperationContext,
            _from: &str,
            _to: &str,
            _args: OpRename,
        ) -> opendal::Result<RpRename> {
            Self::unexpected()
        }

        async fn presign(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            Self::unexpected()
        }
    }

    fn dummy_credential() -> StorageCredential {
        StorageCredential {
            prefix: "memory:/".to_string(),
            config: HashMap::from([("dummy".to_string(), "cred".to_string())]),
        }
    }

    /// Builds a `RefreshableAccessor` whose initial inner accessor is a `FailingAccessor`
    /// (returns `error_kind` on stat), but whose shared storage is a real memory backend.
    /// After credential refresh + rebuild, the accessor switches from `FailingAccessor`
    /// to the real memory backend.
    fn build_refreshable_storage_and_accessor(
        loader: Arc<dyn StorageCredentialsLoader>,
        error_kind: opendal::ErrorKind,
    ) -> RefreshableAccessor {
        let storage = RefreshableOpenDalStorageBuilder::new()
            .scheme("memory".to_string())
            .base_props(HashMap::new())
            .credentials_loader(Arc::clone(&loader))
            .table_ident(TableIdent::new(
                NamespaceIdent::new("test_ns".to_string()),
                "test_table".to_string(),
            ))
            .build()
            .expect("Failed to build storage");

        let info = {
            let inner = storage.lock_inner_storage();
            let path = "memory:/dummy".to_string();
            let (op, _) = inner.create_operator(&path).unwrap();
            let (_ctx, accessor) = op.into_parts();
            accessor.info()
        };

        *storage.lock_cached_info() = Some(info.clone());

        let version = storage.credential_version();
        let failing_accessor: Servicer = Arc::new(FailingAccessor::new(error_kind, info));
        RefreshableAccessor::new(
            failing_accessor,
            version,
            "memory:/dummy".to_string(),
            storage,
        )
    }

    // --- Tests ---

    /// Core retry scenario: when temporary credentials expire mid-operation,
    /// the accessor should transparently refresh and retry.
    ///
    /// Flow:
    /// 1. `get_accessor` → no refresh → FailingAccessor used
    /// 2. `stat` → PermissionDenied
    /// 3. `try_refresh_credentials` → loader call #1 → do_refresh
    /// 4. `rebuild_accessor` → memory accessor used
    /// 5. Memory backend `stat("nonexistent")` → NotFound (not PermissionDenied)
    #[tokio::test]
    async fn test_retry_after_credential_refresh() {
        let loader = Arc::new(SequenceLoader::new(vec![dummy_credential()]));

        let accessor = build_refreshable_storage_and_accessor(
            Arc::clone(&loader) as _,
            opendal::ErrorKind::PermissionDenied,
        );

        let result = accessor
            .stat(&OperationContext::new(), "nonexistent", OpStat::new())
            .await;

        // The retry should have happened — the error should be NotFound
        // (from the memory backend), not PermissionDenied
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            opendal::ErrorKind::NotFound,
            "Expected NotFound after retry, got {:?}",
            err.kind()
        );

        // Only 1 loader call: the retry on PermissionDenied
        assert_eq!(loader.call_count(), 1);
    }

    /// Any error triggers credential retry. When both the original and retry
    /// operations fail, the error message should preserve both.
    ///
    /// Flow:
    /// 1. `get_accessor` → FailingAccessor → Unexpected error
    /// 2. `try_refresh_credentials` → loader call → do_refresh
    /// 3. `rebuild_accessor` → memory accessor
    /// 4. Memory backend `stat("nonexistent")` → NotFound
    /// 5. Final error includes both "Unexpected" original and "NotFound" retry info
    #[tokio::test]
    async fn test_any_error_triggers_retry_and_preserves_both_errors() {
        let loader = Arc::new(SequenceLoader::new(vec![dummy_credential()]));

        let accessor = build_refreshable_storage_and_accessor(
            Arc::clone(&loader) as _,
            opendal::ErrorKind::Unexpected,
        );

        let result = accessor
            .stat(&OperationContext::new(), "nonexistent", OpStat::new())
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        // The retry error kind comes from the memory backend (NotFound)
        assert_eq!(
            err.kind(),
            opendal::ErrorKind::NotFound,
            "Expected NotFound from retry, got {:?}",
            err.kind()
        );

        // Error message should mention both the retry failure and original error
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Original error"),
            "Error should reference original error: {err_msg}"
        );
        assert!(
            err_msg.contains("Unexpected"),
            "Error should contain original Unexpected error kind: {err_msg}"
        );

        // 1 loader call — retry happened
        assert_eq!(loader.call_count(), 1);
    }

    /// When multiple concurrent callers trigger credential refresh, only one
    /// should call the external credential loader. The others should detect the
    /// version bump and skip the loader call.
    #[tokio::test]
    async fn test_concurrent_refresh_calls_loader_only_once() {
        let loader = Arc::new(SequenceLoader::new(vec![dummy_credential()]));

        let storage = RefreshableOpenDalStorageBuilder::new()
            .scheme("memory".to_string())
            .base_props(HashMap::new())
            .credentials_loader(Arc::clone(&loader) as _)
            .table_ident(TableIdent::new(
                NamespaceIdent::new("test_ns".to_string()),
                "test_table".to_string(),
            ))
            .build()
            .expect("Failed to build storage");

        let version = storage.credential_version();

        // Spawn 10 concurrent try_refresh_credentials calls with the same version
        let mut handles = Vec::new();
        for _ in 0..10 {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                storage.try_refresh_credentials(version).await
            }));
        }

        for handle in handles {
            let new_version = handle.await.unwrap().unwrap();
            assert_eq!(new_version, 1, "Version should be 1 after one refresh");
        }

        // Only 1 loader call should have been made
        assert_eq!(loader.call_count(), 1);
    }
}

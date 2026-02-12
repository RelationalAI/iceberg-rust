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

use super::refreshable_storage::RefreshableOpenDalStorage;
use crate::{Error, ErrorKind, Result};

/// An OpenDAL accessor that wraps another accessor and refreshes credentials before operations.
///
/// Each instance has its own inner accessor and shares credential state with
/// other accessors via `Arc<RefreshableOpenDalStorage>`.
pub(crate) struct RefreshableAccessor {
    /// The current backend's accessor (per-instance)
    inner: Mutex<Option<Accessor>>,

    /// Shared storage holding credentials and configuration
    storage: Arc<RefreshableOpenDalStorage>,
}

impl RefreshableAccessor {
    pub(crate) fn new(accessor: Accessor, storage: Arc<RefreshableOpenDalStorage>) -> Self {
        Self {
            inner: Mutex::new(Some(accessor)),
            storage,
        }
    }

    /// Get the current inner accessor (with potential credential refresh).
    ///
    /// If credentials are refreshed, rebuilds the inner accessor from the
    /// updated shared storage.
    async fn get_accessor(&self, path: &str) -> Result<Accessor> {
        let refreshed = self.storage.maybe_refresh().await?;

        if refreshed {
            // Rebuild accessor from the updated shared storage
            let storage_guard = self.storage.inner_storage.lock().unwrap();
            let path_string = path.to_string();
            let (operator, _) = storage_guard.create_operator(&path_string)?;
            drop(storage_guard);

            let new_accessor = operator.into_inner();
            *self.inner.lock().unwrap() = Some(new_accessor.clone());
            return Ok(new_accessor);
        }

        let guard = self.inner.lock().unwrap();
        guard.as_ref().cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Inner accessor not initialized. create_operator must be called first.",
            )
        })
    }

    /// Run an operation with automatic retry on PermissionDenied after credential refresh.
    ///
    /// 1. Gets accessor (which proactively refreshes credentials) and runs the operation.
    /// 2. If it fails with PermissionDenied, calls maybe_refresh to get fresh credentials.
    /// 3. If refresh happened, retries the operation once with the new accessor.
    /// 4. Otherwise, returns the original error.
    async fn with_credential_retry<F, Fut, T>(&self, path: &str, op: F) -> opendal::Result<T>
    where
        F: Fn(Accessor) -> Fut,
        Fut: Future<Output = opendal::Result<T>>,
    {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        let result = op(accessor).await;

        match result {
            Err(err) if err.kind() == opendal::ErrorKind::PermissionDenied => {
                let refreshed = self.storage.maybe_refresh().await.map_err(|e| {
                    opendal::Error::new(
                        opendal::ErrorKind::Unexpected,
                        "Failed to refresh credentials after PermissionDenied",
                    )
                    .set_source(e)
                })?;

                if refreshed {
                    let new_accessor = self.get_accessor(path).await.map_err(|e| {
                        opendal::Error::new(
                            opendal::ErrorKind::Unexpected,
                            "Failed to get accessor after credential refresh",
                        )
                        .set_source(e)
                    })?;
                    op(new_accessor).await
                } else {
                    Err(err)
                }
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

impl Access for RefreshableAccessor {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;

    fn info(&self) -> Arc<AccessorInfo> {
        let info_guard = self.storage.cached_info.lock().unwrap();
        if let Some(info) = info_guard.as_ref() {
            Arc::clone(info)
        } else {
            drop(info_guard);
            AccessorInfo::default().into()
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        self.with_credential_retry(path, |accessor| {
            let args = args.clone();
            async move { accessor.stat(path, args).await }
        })
        .await
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        self.with_credential_retry(path, |accessor| {
            let args = args.clone();
            async move { accessor.read(path, args).await }
        })
        .await
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        self.with_credential_retry(path, |accessor| {
            let args = args.clone();
            async move { accessor.write(path, args).await }
        })
        .await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        self.with_credential_retry("", |accessor| async move { accessor.delete().await })
            .await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.with_credential_retry(path, |accessor| {
            let args = args.clone();
            async move { accessor.list(path, args).await }
        })
        .await
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> opendal::Result<RpCreateDir> {
        self.with_credential_retry(path, |accessor| {
            let args = args.clone();
            async move { accessor.create_dir(path, args).await }
        })
        .await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> opendal::Result<RpRename> {
        self.with_credential_retry(from, |accessor| {
            let args = args.clone();
            async move { accessor.rename(from, to, args).await }
        })
        .await
    }
}

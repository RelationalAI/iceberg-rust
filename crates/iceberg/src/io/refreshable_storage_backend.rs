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
use std::sync::{Arc, Mutex};

use opendal::raw::*;
use opendal::Operator;

use crate::io::{StorageCredential, StorageCredentialsLoader};
use crate::{Error, ErrorKind, Result};

use super::storage::Storage;

/// An OpenDAL backend that wraps another backend and refreshes credentials before operations.
///
/// This backend is transparent - it implements the `Access` trait and delegates all operations
/// to an inner backend after optionally refreshing credentials.
/// The inner backend is created lazily on first access.
#[derive(Clone)]
pub struct RefreshableStorageBackend {
    /// The current backend's accessor (created lazily, rebuilt when credentials refresh)
    inner: Arc<Mutex<Option<Accessor>>>,

    /// Information needed to rebuild the backend
    rebuild_info: Arc<RebuildInfo>,
}

/// Information needed to rebuild the operator when credentials refresh
struct RebuildInfo {
    /// Scheme of the inner backend (e.g., "s3", "azdls")
    scheme: String,

    /// Base properties (non-credential config like endpoint, region, etc.)
    base_props: HashMap<String, String>,

    /// Credential loader
    credentials_loader: Arc<dyn StorageCredentialsLoader>,

    /// Current credentials from last refresh
    current_credentials: Mutex<Option<StorageCredential>>,

    /// Cached AccessorInfo (created lazily from first operator)
    cached_info: Mutex<Option<Arc<AccessorInfo>>>,
}

impl std::fmt::Debug for RefreshableStorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableStorageBackend")
            .field("scheme", &self.rebuild_info.scheme)
            .finish()
    }
}

impl RefreshableStorageBackend {
    /// Creates a new RefreshableStorageBackend
    ///
    /// # Arguments
    /// * `scheme` - Storage scheme (e.g., "s3", "azdls")
    /// * `base_props` - Base configuration properties (without credentials)
    /// * `credentials_loader` - Loader for refreshing credentials
    /// * `initial_credentials` - Initial credentials (if any)
    pub fn new(
        scheme: String,
        base_props: HashMap<String, String>,
        credentials_loader: Arc<dyn StorageCredentialsLoader>,
        initial_credentials: Option<StorageCredential>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            rebuild_info: Arc::new(RebuildInfo {
                scheme,
                base_props,
                credentials_loader,
                current_credentials: Mutex::new(initial_credentials),
                cached_info: Mutex::new(None),
            }),
        }
    }

    /// Check if we should refresh credentials, and if so, rebuild the inner operator
    async fn maybe_refresh(&self, _path: &str) -> Result<()> {
        // TODO: Add refresh condition logic here (time-based, always, etc.)
        // For now, keep it simple - never refresh
        // User said conditions are "hazy" and will be extended later
        let should_refresh = false;

        if should_refresh {
            self.do_refresh(_path).await?;
        }

        Ok(())
    }

    /// Actually refresh credentials and rebuild the operator
    /// This is called both for initial creation and for refreshing existing operator.
    async fn do_refresh(&self, path: &str) -> Result<()> {
        // Get existing credentials (without holding lock across await)
        let existing_creds = {
            let creds_guard = self.rebuild_info.current_credentials.lock().unwrap();
            creds_guard.clone()
        };

        // Load new credentials
        let new_creds = self.rebuild_info
            .credentials_loader
            .load_credentials(path, existing_creds.as_ref())
            .await?;

        // Build new properties by extending base props with credentials
        let mut full_props = self.rebuild_info.base_props.clone();
        full_props.extend(new_creds.config.clone());

        // Build new operator using existing Storage logic
        let new_operator = self.build_operator_from_props(&self.rebuild_info.scheme, full_props)?;

        // Extract the accessor from the new operator
        let new_accessor = new_operator.into_inner();

        // Cache the AccessorInfo if not already cached
        {
            let mut info_guard = self.rebuild_info.cached_info.lock().unwrap();
            if info_guard.is_none() {
                *info_guard = Some(new_accessor.info());
            }
        }

        // Update current accessor and credentials
        *self.inner.lock().unwrap() = Some(new_accessor);
        *self.rebuild_info.current_credentials.lock().unwrap() = Some(new_creds);

        Ok(())
    }

    /// Build an operator from scheme and properties
    fn build_operator_from_props(&self, scheme: &str, props: HashMap<String, String>) -> Result<Operator> {
        // Use existing Storage::build_from_props logic
        let storage = Storage::build_from_props(scheme, props)?;

        // Get an operator from the storage (use "/" as dummy path)
        let dummy_path = "/".to_string();
        let (operator, _) = storage.create_operator(&dummy_path)?;

        Ok(operator)
    }

    /// Get the current inner accessor (with potential refresh)
    /// Creates the accessor lazily on first call.
    async fn get_accessor(&self, path: &str) -> Result<Accessor> {
        // Check if we need to create or refresh
        let needs_creation = {
            let inner_guard = self.inner.lock().unwrap();
            inner_guard.is_none()
        };

        if needs_creation {
            // Create initial operator
            self.do_refresh(path).await?;
        } else {
            // Check if we should refresh
            self.maybe_refresh(path).await?;
        }

        Ok(Arc::clone(self.inner.lock().unwrap().as_ref().unwrap()))
    }
}

impl Access for RefreshableStorageBackend {
    // Use dynamic dispatch for associated types since we don't know
    // the concrete types of the inner backend at compile time
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;

    fn info(&self) -> Arc<AccessorInfo> {
        // Return cached info if available, otherwise create a minimal one
        let info_guard = self.rebuild_info.cached_info.lock().unwrap();
        if let Some(info) = info_guard.as_ref() {
            Arc::clone(info)
        } else {
            // Create a minimal AccessorInfo before first operation
            // This will be replaced with real info on first async operation
            drop(info_guard);
            AccessorInfo::default().into()
        }
    }

    async fn stat(&self, path: &str, args: OpStat) -> opendal::Result<RpStat> {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        accessor.stat(path, args).await
    }

    async fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<(RpRead, Self::Reader)> {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        // Accessor returns already-dynamic types
        accessor.read(path, args).await
    }

    async fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<(RpWrite, Self::Writer)> {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        // Accessor returns already-dynamic types
        accessor.write(path, args).await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        let accessor = self.get_accessor("").await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        // Accessor returns already-dynamic types
        accessor.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        // Accessor returns already-dynamic types
        accessor.list(path, args).await
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> opendal::Result<RpCreateDir> {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        accessor.create_dir(path, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> opendal::Result<RpRename> {
        let accessor = self.get_accessor(from).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        accessor.rename(from, to, args).await
    }

    // Other methods use default implementations (return Unsupported)
}

/// Builder for RefreshableStorageBackend
#[derive(Default, Debug)]
pub struct RefreshableStorageBuilder {
    scheme: Option<String>,
    base_props: HashMap<String, String>,
    credentials_loader: Option<Arc<dyn StorageCredentialsLoader>>,
    initial_credentials: Option<StorageCredential>,
}

impl RefreshableStorageBuilder {
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

    /// Build the RefreshableStorageBackend
    pub fn build(self) -> Result<RefreshableStorageBackend> {
        Ok(RefreshableStorageBackend::new(
            self.scheme.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "scheme is required")
            })?,
            self.base_props,
            self.credentials_loader.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "credentials_loader is required")
            })?,
            self.initial_credentials,
        ))
    }
}


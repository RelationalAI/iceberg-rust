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

use crate::io::{StorageCredential, StorageCredentialsLoader};
use crate::{Error, ErrorKind, Result};

use super::storage::Storage;

/// An OpenDAL backend that wraps another backend and refreshes credentials before operations.
///
/// This backend is transparent - it implements the `Access` trait and delegates all operations
/// to an inner backend after optionally refreshing credentials.
///
/// Each instance has its own inner accessor (not shared across clones).
/// The accessor is created lazily via `refreshable_create_operator` or `do_refresh`.
pub struct RefreshableStorage {
    /// The current backend's accessor (per-instance, created lazily)
    inner: Mutex<Option<Accessor>>,

    /// Shared configuration across clones
    shared: Arc<SharedInfo>,
}

/// Shared configuration for rebuilding operators when credentials refresh.
/// This is shared across clones via Arc.
struct SharedInfo {
    /// Scheme of the inner backend (e.g., "s3", "azdls")
    scheme: String,

    /// Base properties (non-credential config like endpoint, region, etc.)
    base_props: HashMap<String, String>,

    /// Inner storage (built in new, rebuilt on credential refresh)
    inner_storage: Mutex<Box<Storage>>,

    /// Credential loader
    credentials_loader: Arc<dyn StorageCredentialsLoader>,

    /// Current credentials from last refresh (shared across clones)
    current_credentials: Mutex<Option<StorageCredential>>,

    /// Cached AccessorInfo (created lazily from first operator)
    cached_info: Mutex<Option<Arc<AccessorInfo>>>,
}

impl Clone for RefreshableStorage {
    fn clone(&self) -> Self {
        Self {
            inner: Mutex::new(None),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl std::fmt::Debug for RefreshableStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableStorage")
            .finish()
    }
}

impl RefreshableStorage {
    /// Creates a new RefreshableStorage.
    ///
    /// This only stores configuration. No storage or accessor is built here.
    /// The inner accessor is created lazily via `refreshable_create_operator`.
    ///
    /// # Arguments
    /// * `scheme` - Storage scheme (e.g., "s3", "azdls")
    /// * `base_props` - Base configuration properties (without credentials)
    /// * `credentials_loader` - Loader for refreshing credentials
    /// * `initial_credentials` - Initial credentials (if any), stored as current_credentials
    pub fn new(
        scheme: String,
        base_props: HashMap<String, String>,
        credentials_loader: Arc<dyn StorageCredentialsLoader>,
        initial_credentials: Option<StorageCredential>,
    ) -> Result<Self> {
        // Build initial inner_storage from base_props + initial_credentials
        let mut props = base_props.clone();
        if let Some(ref creds) = initial_credentials {
            props.extend(creds.config.clone());
        }
        let inner_storage = Storage::build_from_props(&scheme, props)?;

        Ok(Self {
            inner: Mutex::new(None),
            shared: Arc::new(SharedInfo {
                scheme,
                base_props,
                inner_storage: Mutex::new(Box::new(inner_storage)),
                credentials_loader,
                current_credentials: Mutex::new(initial_credentials),
                cached_info: Mutex::new(None),
            }),
        })
    }

    /// Build an inner storage from props, create an operator from it to extract the
    /// relative path, and store the inner accessor on this instance.
    ///
    /// Props are built from base_props + current_credentials (if any).
    pub fn refreshable_create_operator(&self, path: &str) -> Result<String> {
        // Use shared inner_storage to create operator
        let storage_guard = self.shared.inner_storage.lock().unwrap();
        let path_string = path.to_string();
        let (operator, relative_path) = storage_guard.create_operator(&path_string)?;
        let relative_path = relative_path.to_string();
        drop(storage_guard);

        // Store the accessor
        let accessor = operator.into_inner();

        // Cache AccessorInfo if not already cached
        {
            let mut info_guard = self.shared.cached_info.lock().unwrap();
            if info_guard.is_none() {
                *info_guard = Some(accessor.info());
            }
        }

        *self.inner.lock().unwrap() = Some(accessor);

        Ok(relative_path)
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

    /// Actually refresh credentials and rebuild the operator.
    /// Builds new inner storage from base_props + new credentials,
    /// creates a new inner accessor from it, and saves new current_credentials.
    async fn do_refresh(&self, path: &str) -> Result<()> {
        // Get existing credentials (without holding lock across await)
        let existing_creds = {
            let creds_guard = self.shared.current_credentials.lock().unwrap();
            creds_guard.clone()
        };

        // Load new credentials
        let new_creds = self.shared
            .credentials_loader
            .load_credentials(path, existing_creds.as_ref())
            .await?;

        // Build new properties by extending base props with credentials
        let mut full_props = self.shared.base_props.clone();
        full_props.extend(new_creds.config.clone());

        // Build new inner storage and accessor
        let new_storage = Storage::build_from_props(&self.shared.scheme, full_props)?;
        let dummy_path = "/".to_string();
        let (new_operator, _) = new_storage.create_operator(&dummy_path)?;
        let new_accessor = new_operator.into_inner();

        // Cache the AccessorInfo if not already cached
        {
            let mut info_guard = self.shared.cached_info.lock().unwrap();
            if info_guard.is_none() {
                *info_guard = Some(new_accessor.info());
            }
        }

        // Update current accessor, inner storage, and credentials
        *self.inner.lock().unwrap() = Some(new_accessor);
        *self.shared.inner_storage.lock().unwrap() = Box::new(new_storage);
        *self.shared.current_credentials.lock().unwrap() = Some(new_creds);

        Ok(())
    }

    /// Get the current inner accessor (with potential refresh)
    async fn get_accessor(&self, path: &str) -> Result<Accessor> {
        self.maybe_refresh(path).await?;

        let guard = self.inner.lock().unwrap();
        guard.as_ref().cloned().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Inner accessor not initialized. refreshable_create_operator must be called first.")
        })
    }
}

impl Access for RefreshableStorage {
    // Use dynamic dispatch for associated types since we don't know
    // the concrete types of the inner backend at compile time
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;

    fn info(&self) -> Arc<AccessorInfo> {
        // Return cached info if available, otherwise create a minimal one
        let info_guard = self.shared.cached_info.lock().unwrap();
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

        accessor.write(path, args).await
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        let accessor = self.get_accessor("").await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

        accessor.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        let accessor = self.get_accessor(path).await.map_err(|e| {
            opendal::Error::new(opendal::ErrorKind::Unexpected, "Failed to get accessor")
                .set_source(e)
        })?;

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

/// Builder for RefreshableStorage
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

    /// Build the RefreshableStorage
    pub fn build(self) -> Result<RefreshableStorage> {
        RefreshableStorage::new(
            self.scheme.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "scheme is required")
            })?,
            self.base_props,
            self.credentials_loader.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "credentials_loader is required")
            })?,
            self.initial_credentials,
        )
    }
}

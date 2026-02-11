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

use crate::io::file_io::Extensions;
use crate::io::{StorageCredential, StorageCredentialsLoader};
use crate::{Error, ErrorKind, Result};

use super::refreshable_accessor::RefreshableAccessor;
use super::storage::Storage;

/// Holds shared configuration and state for credential refresh.
///
/// Multiple `RefreshableAccessor` instances share a single `RefreshableStorage`
/// via `Arc`, allowing credential refreshes to be visible across all accessors.
pub struct RefreshableStorage {
    /// Scheme of the inner backend (e.g., "s3", "azdls")
    scheme: String,

    /// Base properties (non-credential config like endpoint, region, etc.)
    base_props: HashMap<String, String>,

    /// Inner storage (built in new, rebuilt on credential refresh)
    pub(crate) inner_storage: Mutex<Box<Storage>>,

    /// Credential loader
    credentials_loader: Arc<dyn StorageCredentialsLoader>,

    /// Extensions for building storage (e.g. custom S3 credential loaders)
    extensions: Extensions,

    /// Current credentials from last refresh (shared across accessors)
    current_credentials: Mutex<Option<StorageCredential>>,

    /// Cached AccessorInfo (created lazily from first operator)
    pub(crate) cached_info: Mutex<Option<Arc<AccessorInfo>>>,
}

impl std::fmt::Debug for RefreshableStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshableStorage").finish()
    }
}

impl RefreshableStorage {
    /// Creates a new RefreshableStorage.
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
        let inner_storage = Storage::build_from_props(&scheme, props, &extensions)?;

        Ok(Self {
            scheme,
            base_props,
            inner_storage: Mutex::new(Box::new(inner_storage)),
            credentials_loader,
            extensions,
            current_credentials: Mutex::new(initial_credentials),
            cached_info: Mutex::new(None),
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

        let refreshable_accessor = RefreshableAccessor::new(accessor, Arc::clone(self));

        let wrapped_operator = Operator::from_inner(Arc::new(refreshable_accessor));
        Ok((wrapped_operator, relative_path))
    }

    /// Load credentials if available, and refresh inner storage if new ones are returned.
    ///
    /// Returns `Ok(true)` if credentials were refreshed, `Ok(false)` otherwise.
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

    /// Rebuild inner storage from new credentials.
    fn do_refresh(&self, new_creds: StorageCredential) -> Result<()> {
        let mut full_props = self.base_props.clone();
        full_props.extend(new_creds.config.clone());

        let new_storage =
            Storage::build_from_props(&self.scheme, full_props, &self.extensions)?;

        *self.inner_storage.lock().unwrap() = Box::new(new_storage);
        *self.current_credentials.lock().unwrap() = Some(new_creds);

        Ok(())
    }
}

/// Builder for RefreshableStorage
#[derive(Default, Debug)]
pub struct RefreshableStorageBuilder {
    scheme: Option<String>,
    base_props: HashMap<String, String>,
    credentials_loader: Option<Arc<dyn StorageCredentialsLoader>>,
    initial_credentials: Option<StorageCredential>,
    extensions: Extensions,
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

    /// Set the extensions
    pub fn extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Build the RefreshableStorage wrapped in Arc
    pub fn build(self) -> Result<Arc<RefreshableStorage>> {
        Ok(Arc::new(RefreshableStorage::new(
            self.scheme.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "scheme is required")
            })?,
            self.base_props,
            self.credentials_loader.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "credentials_loader is required")
            })?,
            self.initial_credentials,
            self.extensions,
        )?))
    }
}

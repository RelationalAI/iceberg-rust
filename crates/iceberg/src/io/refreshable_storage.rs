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

use opendal::Operator;

use crate::io::{StorageCredential, StorageCredentialsLoader};
use crate::Result;

use super::storage::Storage;

/// A storage wrapper that refreshes credentials before each operation.
///
/// This wraps an existing storage implementation and refreshes credentials
/// by calling a `StorageCredentialsLoader` before delegating to the inner storage.
#[derive(Debug)]
pub(crate) struct RefreshableStorage {
    /// Base properties (non-credential config like endpoint, region)
    base_props: HashMap<String, String>,

    /// Storage scheme (s3, abfss, etc.)
    scheme: String,

    /// Credential loader from extension
    credentials_loader: Arc<dyn StorageCredentialsLoader>,

    /// Current credentials from last refresh (Mutex for thread-safety)
    existing_credentials: Arc<Mutex<Option<StorageCredential>>>,
}

impl RefreshableStorage {
    /// Creates a new `RefreshableStorage` wrapper.
    ///
    /// # Arguments
    /// * `base_props` - Base configuration properties
    /// * `scheme` - Storage scheme string (e.g., "s3", "abfss")
    /// * `credentials_loader` - Loader for refreshing credentials
    /// * `existing_credentials` - Initial credentials (if any)
    pub(crate) fn new(
        base_props: HashMap<String, String>,
        scheme: String,
        credentials_loader: Arc<dyn StorageCredentialsLoader>,
        existing_credentials: Option<StorageCredential>,
    ) -> Self {
        Self {
            base_props,
            scheme,
            credentials_loader,
            existing_credentials: Arc::new(Mutex::new(existing_credentials)),
        }
    }

    /// Creates an operator for the given path, refreshing credentials first.
    ///
    /// This method:
    /// 1. Locks the existing credentials mutex
    /// 2. Refreshes credentials using the loader
    /// 3. Extends base properties with refreshed credentials
    /// 4. Rebuilds storage with the new properties
    /// 5. Updates existing credentials
    /// 6. Delegates to the new storage's create_operator
    ///
    /// # Arguments
    /// * `path` - The absolute path to access
    ///
    /// # Returns
    /// A tuple of (Operator, relative_path)
    pub(crate) fn create_operator(
        &self,
        path: &str,
    ) -> Result<(Operator, String)> {
        // Lock for entire refresh + update operation (atomic)
        let mut existing_creds_guard = self.existing_credentials.lock().unwrap();

        // 1. Refresh credentials (using locked existing credentials) - block on async operation
        let refreshed_cred = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(self.credentials_loader.load_credentials(path, existing_creds_guard.as_ref()))?,
            Err(_) => {
                // No runtime exists, create a temporary one
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    crate::Error::new(crate::ErrorKind::Unexpected, "Failed to create Tokio runtime")
                        .with_source(e)
                })?;
                rt.block_on(self.credentials_loader.load_credentials(path, existing_creds_guard.as_ref()))?
            }
        };

        // 2. Extend base properties with refreshed credentials
        let mut full_props = self.base_props.clone();
        full_props.extend(refreshed_cred.config.clone());

        // 3. Update existing credentials (still holding lock)
        *existing_creds_guard = Some(refreshed_cred);

        // 4. Release lock before delegating
        drop(existing_creds_guard);

        // 5. Rebuild storage from extended properties
        let new_storage = Storage::build_from_props(&self.scheme, full_props)?;

        // 6. Delegate to new storage
        let path_string = path.to_string();
        let (operator, relative_path) = new_storage.create_operator(&path_string)?;
        Ok((operator, relative_path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct MockCredentialLoader {
        call_count: Arc<AtomicUsize>,
        returned_config: HashMap<String, String>,
    }

    impl MockCredentialLoader {
        fn new(config: HashMap<String, String>) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                returned_config: config,
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for MockCredentialLoader {
        async fn load_credentials(
            &self,
            _location: &str,
            _existing_credentials: Option<&StorageCredential>,
        ) -> Result<StorageCredential> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(StorageCredential {
                prefix: "s3://test-bucket/".to_string(),
                config: self.returned_config.clone(),
            })
        }
    }

    #[test]
    fn test_refreshable_storage_refreshes_credentials() {
        let mut base_props = HashMap::new();
        base_props.insert("region".to_string(), "us-east-1".to_string());

        let mut cred_config = HashMap::new();
        cred_config.insert("access_key_id".to_string(), "test-key".to_string());
        cred_config.insert("secret_access_key".to_string(), "test-secret".to_string());

        let loader = Arc::new(MockCredentialLoader::new(cred_config));
        let loader_clone = Arc::clone(&loader);

        let refreshable = RefreshableStorage::new(
            base_props,
            "s3".to_string(),
            loader_clone,
            None,
        );

        // First call should trigger credential refresh
        let _result = refreshable.create_operator("s3://test-bucket/path/file.parquet");

        // Should fail because we don't have a real S3 setup, but that's okay
        // We just want to verify the loader was called
        assert_eq!(loader.call_count(), 1);
    }

    #[test]
    fn test_refreshable_storage_passes_existing_credentials() {
        let base_props: HashMap<String, String> = HashMap::new();

        #[derive(Debug)]
        struct ExistingCredChecker {
            received_existing: Arc<Mutex<Option<bool>>>,
        }

        #[async_trait::async_trait]
        impl StorageCredentialsLoader for ExistingCredChecker {
            async fn load_credentials(
                &self,
                _location: &str,
                existing_credentials: Option<&StorageCredential>,
            ) -> Result<StorageCredential> {
                let mut guard = self.received_existing.lock().unwrap();
                *guard = Some(existing_credentials.is_some());

                Ok(StorageCredential {
                    prefix: "s3://test/".to_string(),
                    config: HashMap::new(),
                })
            }
        }

        let checker = Arc::new(ExistingCredChecker {
            received_existing: Arc::new(Mutex::new(None)),
        });
        let checker_clone = Arc::clone(&checker);

        let initial_cred = StorageCredential {
            prefix: "s3://initial/".to_string(),
            config: HashMap::new(),
        };

        let refreshable = RefreshableStorage::new(
            base_props,
            "s3".to_string(),
            checker_clone,
            Some(initial_cred),
        );

        let _ = refreshable.create_operator("s3://test/file.parquet");

        // Verify existing credentials were passed
        let received = checker.received_existing.lock().unwrap();
        assert_eq!(*received, Some(true));
    }

    #[test]
    fn test_refreshable_storage_concurrent_access() {
        let base_props: HashMap<String, String> = HashMap::new();
        let mut cred_config: HashMap<String, String> = HashMap::new();
        cred_config.insert("key".to_string(), "value".to_string());

        let loader = Arc::new(MockCredentialLoader::new(cred_config));
        let loader_clone = Arc::clone(&loader);

        let refreshable = Arc::new(RefreshableStorage::new(
            base_props,
            "s3".to_string(),
            loader_clone,
            None,
        ));

        // Spawn multiple concurrent threads
        let mut handles = vec![];
        for i in 0..5 {
            let refreshable_clone = Arc::clone(&refreshable);
            let handle = std::thread::spawn(move || {
                let path = format!("s3://test-bucket/path{}/file.parquet", i);
                refreshable_clone.create_operator(&path)
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            let _ = handle.join();
        }

        // Verify loader was called for each operation
        assert_eq!(loader.call_count(), 5);
    }
}

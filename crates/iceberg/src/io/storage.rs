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

//! Storage interfaces for Iceberg.

use std::fmt::Debug;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile};
use crate::Result;
pub use crate::io::config::StorageConfig;

/// Trait for storage operations in Iceberg.
///
/// The trait supports serialization via `typetag`, allowing storage instances to be
/// serialized and deserialized across process boundaries.
///
/// Third-party implementations can implement this trait to provide custom storage backends.
///
/// # Implementing Custom Storage
///
/// To implement a custom storage backend:
///
/// 1. Create a struct that implements this trait
/// 2. Add `#[typetag::serde]` attribute for serialization support
/// 3. Implement all required methods
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyStorage {
///     // custom fields
/// }
///
/// #[async_trait]
/// #[typetag::serde]
/// impl Storage for MyStorage {
///     async fn exists(&self, path: &str) -> Result<bool> {
///         // implementation
///         todo!()
///     }
///     // ... implement other methods
/// }
///
/// TODO remove below when the trait is integrated with FileIO and Catalog
/// # NOTE
/// This trait is under heavy development and is not used anywhere as of now
/// Please DO NOT implement it
/// ```
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Get metadata from an input path
    async fn metadata(&self, path: &str) -> Result<FileMetadata>;

    /// Read bytes from a path
    async fn read(&self, path: &str) -> Result<Bytes>;

    /// Get FileRead from a path
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>>;

    /// Write bytes to an output path
    async fn write(&self, path: &str, bs: Bytes) -> Result<()>;

    /// Get FileWrite from a path
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    /// Delete a file at the given path
    async fn delete(&self, path: &str) -> Result<()>;

    /// Delete all files with the given prefix
    async fn delete_prefix(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFile>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFile>;
}

/// Factory for creating Storage instances from configuration.
///
/// Implement this trait to provide custom storage backends. The factory pattern
/// allows for lazy initialization of storage instances and enables users to
/// inject custom storage implementations into catalogs.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// struct MyCustomStorageFactory {
///     // custom configuration
/// }
///
/// #[typetag::serde]
/// impl StorageFactory for MyCustomStorageFactory {
///     fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
///         // Create and return custom storage implementation
///         todo!()
///     }
/// }
///
/// TODO remove below when the trait is integrated with FileIO and Catalog
/// # NOTE
/// This trait is under heavy development and is not used anywhere as of now
/// Please DO NOT implement it
/// ```
#[typetag::serde(tag = "type")]
pub trait StorageFactory: Debug + Send + Sync {
    /// Build a new Storage instance from the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration containing scheme and properties
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Arc<dyn Storage>` on success, or an error
    /// if the storage could not be created.
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIOBuilder, StorageCredential, StorageCredentialsLoader};

    #[derive(Debug)]
    struct TestCredentialLoader;

    #[async_trait::async_trait]
    impl StorageCredentialsLoader for TestCredentialLoader {
        async fn maybe_load_credentials(
            &self,
            _location: &str,
            _existing_credentials: Option<&StorageCredential>,
        ) -> crate::Result<Option<StorageCredential>> {
            Ok(Some(StorageCredential {
                prefix: "s3://test/".to_string(),
                config: HashMap::new(),
            }))
        }
    }

    #[test]
    fn test_storage_build_with_credentials_loader_creates_refreshable() {
        let loader: Arc<dyn StorageCredentialsLoader> = Arc::new(TestCredentialLoader);

        let file_io_builder = FileIOBuilder::new("s3")
            .with_prop("bucket", "test-bucket")
            .with_extension(loader);

        let storage = Storage::build(file_io_builder).unwrap();

        // Verify it created a Refreshable variant
        match storage {
            Storage::Refreshable { .. } => {} // Success
            _ => panic!("Expected Refreshable variant"),
        }
    }

    #[test]
    fn test_storage_build_without_loader_creates_normal_storage() {
        #[cfg(feature = "storage-memory")]
        {
            let file_io_builder = FileIOBuilder::new("memory");
            let storage = Storage::build(file_io_builder).unwrap();

            // Verify it created a normal Memory variant
            match storage {
                Storage::Memory(_) => {} // Success
                _ => panic!("Expected Memory variant"),
            }
        }
    }

    #[test]
    fn test_storage_build_with_both_loader_and_initial_credentials() {
        let loader: Arc<dyn StorageCredentialsLoader> = Arc::new(TestCredentialLoader);
        let initial_cred = StorageCredential {
            prefix: "s3://initial/".to_string(),
            config: HashMap::new(),
        };

        let file_io_builder = FileIOBuilder::new("s3")
            .with_prop("bucket", "test-bucket")
            .with_extension(loader)
            .with_extension(initial_cred);

        let storage = Storage::build(file_io_builder).unwrap();

        // Verify it created a Refreshable variant
        match storage {
            Storage::Refreshable { .. } => {} // Success
            _ => panic!("Expected Refreshable variant"),
        }
    }

    #[test]
    fn test_storage_build_from_props_never_creates_refreshable() {
        let mut props = HashMap::new();
        props.insert("bucket".to_string(), "test-bucket".to_string());

        #[cfg(feature = "storage-s3")]
        {
            let storage = Storage::build_from_props("s3", props, &Default::default()).unwrap();

            // Verify it created a normal S3 variant, not Refreshable
            match storage {
                Storage::S3 { .. } => {} // Success
                Storage::Refreshable { .. } => panic!("build_from_props should not create Refreshable"),
                _ => panic!("Expected S3 variant"),
            }
        }
    }
}

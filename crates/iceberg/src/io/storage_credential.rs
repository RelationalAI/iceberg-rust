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
use std::fmt::Debug;

use crate::Result;
use crate::catalog::TableIdent;

/// Storage credentials for accessing cloud storage.
///
/// Contains configuration properties like access keys, tokens, etc.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageCredential {
    /// Prefix for which these credentials are valid
    pub prefix: String,
    /// Configuration properties for the storage credentials
    pub config: HashMap<String, String>,
}

/// Newtype wrapper for the metadata location string, used as an extension
/// so that `RefreshableOpenDalStorage` can pass it to `load_credentials`.
#[derive(Debug, Clone)]
pub struct MetadataLocation(pub String);

/// Trait for loading storage credentials dynamically.
///
/// Implementations can fetch credentials from external sources,
/// refresh expired credentials, or implement custom credential logic.
///
/// # Example
///
/// ```rust,no_run
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// use iceberg::io::{StorageCredential, StorageCredentialsLoader};
///
/// #[derive(Debug)]
/// struct MyCredentialLoader;
///
/// #[async_trait::async_trait]
/// impl StorageCredentialsLoader for MyCredentialLoader {
///     async fn load_credentials(
///         &self,
///         _table_ident: &iceberg::TableIdent,
///         _location: &str,
///     ) -> iceberg::Result<StorageCredential> {
///         // Fetch fresh credentials from your credential service
///         let mut config = HashMap::new();
///         config.insert("access_key_id".to_string(), "fresh-key".to_string());
///         config.insert("secret_access_key".to_string(), "fresh-secret".to_string());
///
///         Ok(StorageCredential {
///             prefix: "s3://my-bucket/".to_string(),
///             config,
///         })
///     }
/// }
///
/// // The loader is passed to the catalog configuration (e.g., RestCatalogConfig),
/// // which creates storage instances with automatic credential refresh.
/// let loader: Arc<dyn StorageCredentialsLoader> = Arc::new(MyCredentialLoader);
/// ```
#[async_trait::async_trait]
pub trait StorageCredentialsLoader: Send + Sync + Debug {
    /// Load storage credentials using custom user-defined logic.
    ///
    /// # Arguments
    /// * `table_ident` - The table identifier for which credentials are being loaded
    /// * `location` - The full path being accessed (e.g., "s3://bucket/path/file.parquet")
    async fn load_credentials(
        &self,
        table_ident: &TableIdent,
        location: &str,
    ) -> Result<StorageCredential>;
}

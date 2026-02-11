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

use iceberg::io::{FileIOBuilder, StorageCredential, StorageCredentialsLoader};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct TestCredentialLoader;

#[async_trait::async_trait]
impl StorageCredentialsLoader for TestCredentialLoader {
    async fn maybe_load_credentials(
        &self,
        _location: &str,
        _existing_credentials: Option<&StorageCredential>,
    ) -> iceberg::Result<Option<StorageCredential>> {
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

    // Building FileIO with a credentials loader should create refreshable storage
    let file_io = file_io_builder.build();
    assert!(file_io.is_ok(), "FileIO should build successfully with credentials loader");
}

#[test]
fn test_storage_build_without_loader_creates_normal_storage() {
    #[cfg(feature = "storage-memory")]
    {
        let file_io_builder = FileIOBuilder::new("memory");
        let file_io = file_io_builder.build();

        assert!(file_io.is_ok(), "FileIO should build successfully without loader");
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

    let file_io = file_io_builder.build();
    assert!(file_io.is_ok(), "FileIO should build with both loader and initial credentials");
}

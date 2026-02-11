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
use std::sync::Arc;

use opendal::layers::RetryLayer;
#[cfg(feature = "storage-azdls")]
use opendal::services::AzdlsConfig;
#[cfg(feature = "storage-gcs")]
use opendal::services::GcsConfig;
#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};

#[cfg(feature = "storage-azdls")]
use super::AzureStorageScheme;
use super::FileIOBuilder;
#[cfg(feature = "storage-s3")]
use crate::io::CustomAwsCredentialLoader;
use crate::io::{StorageCredential, StorageCredentialsLoader};
use crate::{Error, ErrorKind};

/// The storage carries all supported storage services in iceberg
#[derive(Debug)]
pub(crate) enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory(Operator),
    #[cfg(feature = "storage-fs")]
    LocalFs,
    /// Expects paths of the form `s3[a]://<bucket>/<path>`.
    #[cfg(feature = "storage-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        config: Arc<S3Config>,
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },
    #[cfg(feature = "storage-oss")]
    Oss { config: Arc<OssConfig> },
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "storage-azdls")]
    Azdls {
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: AzureStorageScheme,
        config: Arc<AzdlsConfig>,
    },
    /// Wraps any storage with credential refresh capability.
    Refreshable {
        backend: Arc<super::refreshable_storage::RefreshableStorage>,
    },
}

impl Storage {
    /// Convert iceberg config to opendal config.
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();

        // Check if credential refresh is requested
        let credentials_loader = extensions.get::<Arc<dyn StorageCredentialsLoader>>();
        let existing_credentials = extensions.get::<StorageCredential>();

        // If loader is present, create RefreshableStorage immediately
        if let Some(loader) = credentials_loader {
            let backend = super::refreshable_storage::RefreshableStorageBuilder::new()
                .scheme(scheme_str.clone())
                .base_props(props)
                .credentials_loader(Arc::clone(&loader))
                .initial_credentials(existing_credentials.map(|c| (*c).clone()))
                .extensions(extensions)
                .build()?;

            return Ok(Storage::Refreshable { backend });
        }

        // Otherwise, build storage normally
        Self::build_from_props(&scheme_str, props, &extensions)
    }

    /// Build storage from scheme, properties, and extensions.
    ///
    /// This is the core builder used by both `build()` and `RefreshableStorage`.
    pub(crate) fn build_from_props(
        scheme_str: &str,
        props: HashMap<String, String>,
        extensions: &super::file_io::Extensions,
    ) -> crate::Result<Self> {
        let scheme = Self::parse_scheme(scheme_str)?;
        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory(super::memory_config_build()?)),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                configured_scheme: scheme_str.to_string(),
                config: super::s3_config_parse(props)?.into(),
                customized_credential_load: extensions
                    .get::<CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone),
            }),
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => Ok(Self::Gcs {
                config: super::gcs_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => Ok(Self::Oss {
                config: super::oss_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let scheme = scheme_str.parse::<AzureStorageScheme>()?;
                Ok(Self::Azdls {
                    config: super::azdls_config_parse(props)?.into(),
                    configured_scheme: scheme,
                })
            }
            // Update doc on [`FileIO`] when adding new schemes.
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Constructing file io from scheme: {scheme} not supported now",),
            )),
        }
    }

    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    pub(crate) fn create_operator<'a>(
        &'a self,
        path: &'a impl AsRef<str>,
    ) -> crate::Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok::<_, crate::Error>((op.clone(), stripped))
                } else {
                    Ok::<_, crate::Error>((op.clone(), &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok::<_, crate::Error>((op, stripped))
                } else {
                    Ok::<_, crate::Error>((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 {
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op = super::s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "storage-gcs")]
            Storage::Gcs { config } => {
                let operator = super::gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    Ok((operator, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {}, should start with {}", path, prefix),
                    ))
                }
            }
            #[cfg(feature = "storage-oss")]
            Storage::Oss { config } => {
                let op = super::oss_config_build(config, path)?;

                // Check prefix of oss path.
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {}, should start with {}", path, prefix),
                    ))
                }
            }
            #[cfg(feature = "storage-azdls")]
            Storage::Azdls {
                configured_scheme,
                config,
            } => super::azdls_create_operator(path, config, configured_scheme),
            Storage::Refreshable { backend } => {
                let (operator, relative_path) = backend.refreshable_create_operator(path)?;
                // relative_path is always a suffix of `path`, so slice the
                // input directly to avoid an allocation.
                let relative_path_ref = &path[path.len() - relative_path.len()..];
                Ok((operator, relative_path_ref))
            }
            #[cfg(all(
                not(feature = "storage-s3"),
                not(feature = "storage-fs"),
                not(feature = "storage-gcs"),
                not(feature = "storage-oss"),
                not(feature = "storage-azdls"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }?;

        // Transient errors are common for object stores; however there's no
        // harm in retrying temporary failures for other storage backends as well.
        let operator = operator.layer(RetryLayer::new());

        Ok((operator, relative_path))
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "gs" | "gcs" => Ok(Scheme::Gcs),
            "oss" => Ok(Scheme::Oss),
            "abfss" | "abfs" | "wasbs" | "wasb" => Ok(Scheme::Azdls),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
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

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

//! REST-catalog-specific integration tests for the custom authenticator.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{cleanup_namespace_dyn, rest_catalog_with_auth};
use iceberg::{Catalog, NamespaceIdent, Result as IcebergResult};
use iceberg_catalog_rest::CustomAuthenticator;
use iceberg_test_utils::set_up;

#[derive(Debug)]
struct CountingAuthenticator {
    count: Arc<Mutex<usize>>,
}

#[async_trait]
impl CustomAuthenticator for CountingAuthenticator {
    async fn get_token(&self) -> IcebergResult<String> {
        let mut c = self.count.lock().unwrap();
        *c += 1;
        Ok(format!("token_{}", *c))
    }
}

#[tokio::test]
async fn test_authenticator_token_refresh() {
    set_up();

    let token_request_count = Arc::new(Mutex::new(0));
    let authenticator = Arc::new(CountingAuthenticator {
        count: token_request_count.clone(),
    });

    let catalog = rest_catalog_with_auth(Some(authenticator)).await;

    let ns1_ident = NamespaceIdent::from_strs(["test_refresh_1"]).unwrap();
    let ns2_ident = NamespaceIdent::from_strs(["test_refresh_2"]).unwrap();
    cleanup_namespace_dyn(&catalog, &ns1_ident).await;
    cleanup_namespace_dyn(&catalog, &ns2_ident).await;

    catalog
        .create_namespace(&ns1_ident, HashMap::new())
        .await
        .unwrap();
    catalog
        .create_namespace(&ns2_ident, HashMap::new())
        .await
        .unwrap();

    // With lazy authentication, the token is fetched once and cached for reuse
    // across multiple operations, rather than being called on every request
    let count = *token_request_count.lock().unwrap();
    assert_eq!(
        count, 1,
        "Authenticator should have been called once for lazy token caching, but was called {count} times"
    );

    // Test that token is refreshed when invalidated
    catalog.invalidate_token().await.unwrap();

    let ns3_ident = NamespaceIdent::from_strs(["test_refresh_3"]).unwrap();
    cleanup_namespace_dyn(&catalog, &ns3_ident).await;
    catalog
        .create_namespace(&ns3_ident, HashMap::new())
        .await
        .unwrap();

    // After invalidating and making another request, authenticator should be called again
    let count = *token_request_count.lock().unwrap();
    assert_eq!(
        count, 2,
        "Authenticator should have been called twice (once initial, once after invalidation), but was called {count} times"
    );

    cleanup_namespace_dyn(&catalog, &ns1_ident).await;
    cleanup_namespace_dyn(&catalog, &ns2_ident).await;
    cleanup_namespace_dyn(&catalog, &ns3_ident).await;
}

#[tokio::test]
async fn test_authenticator_persists_across_operations() {
    set_up();

    let operation_count = Arc::new(Mutex::new(0));
    let authenticator = Arc::new(CountingAuthenticator {
        count: operation_count.clone(),
    });

    let catalog = rest_catalog_with_auth(Some(authenticator)).await;

    let ns_ident = NamespaceIdent::from_strs(["test_persist", "auth"]).unwrap();
    let parent_ident = NamespaceIdent::from_strs(["test_persist"]).unwrap();
    cleanup_namespace_dyn(&catalog, &ns_ident).await;

    catalog
        .create_namespace(&ns_ident, HashMap::new())
        .await
        .unwrap();

    let count_after_create = *operation_count.lock().unwrap();

    let list_result = catalog.list_namespaces(Some(&parent_ident)).await.unwrap();
    assert!(
        list_result.contains(&ns_ident),
        "Namespace {:?} not found in list {:?}",
        ns_ident,
        list_result
    );

    let count_after_list = *operation_count.lock().unwrap();

    // With lazy authentication, the token is fetched once on the first operation
    // and then reused for subsequent operations without calling the authenticator again
    assert_eq!(
        count_after_create, 1,
        "Authenticator should be called once for the create operation"
    );
    assert_eq!(
        count_after_list, 1,
        "Authenticator should still have been called only once (token is cached and reused for list)"
    );

    cleanup_namespace_dyn(&catalog, &ns_ident).await;
}

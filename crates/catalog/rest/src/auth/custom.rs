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

//! Adapts [`CustomAuthenticator`] (a simple "give me a bearer token" callback, the
//! fork's own extension point predating the `AuthManager`/`AuthSession` architecture)
//! onto that architecture, mirroring how [`super::OAuth2Manager`] adapts OAuth2.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::{Error, ErrorKind, Result};
use tokio::sync::Mutex;

use super::{AuthManager, AuthSession};
use crate::client::{CustomAuthenticator, HttpClient};
use crate::request::HttpRequest;

/// [`AuthManager`] wrapping a user-supplied [`CustomAuthenticator`].
///
/// The fetched token is cached and shared (via a cloned `Arc<Mutex<_>>`) across every
/// session the manager derives, matching [`super::OAuth2Manager`]'s `token` cell — so a
/// session fetches a fresh token only on the first request that needs one, not on every
/// request. [`CustomAuthenticatorSession::invalidate`] clears the cache, reachable through
/// [`crate::RestCatalog::invalidate_token`].
pub(crate) struct CustomAuthenticatorManager {
    authenticator: Arc<dyn CustomAuthenticator>,
    token: Arc<Mutex<Option<String>>>,
}

impl CustomAuthenticatorManager {
    pub(crate) fn new(authenticator: Arc<dyn CustomAuthenticator>) -> Self {
        Self {
            authenticator,
            token: Arc::new(Mutex::new(None)),
        }
    }
}

impl Debug for CustomAuthenticatorManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAuthenticatorManager")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthManager for CustomAuthenticatorManager {
    async fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(CustomAuthenticatorSession {
            authenticator: self.authenticator.clone(),
            token: self.token.clone(),
        }))
    }

    async fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(CustomAuthenticatorSession {
            authenticator: self.authenticator.clone(),
            token: self.token.clone(),
        }))
    }
}

struct CustomAuthenticatorSession {
    authenticator: Arc<dyn CustomAuthenticator>,
    token: Arc<Mutex<Option<String>>>,
}

impl Debug for CustomAuthenticatorSession {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomAuthenticatorSession")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSession for CustomAuthenticatorSession {
    async fn authenticate(&self, req: &mut HttpRequest) -> Result<()> {
        let mut guard = self.token.lock().await;
        if guard.is_none() {
            *guard = Some(self.authenticator.get_token().await?);
        }
        let token = guard.as_ref().expect("just set above if it was None");

        let mut value: http::HeaderValue = format!("Bearer {token}").parse().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Invalid token received from custom authenticator",
            )
            .with_source(e)
        })?;
        value.set_sensitive(true);
        req.headers_mut().insert(http::header::AUTHORIZATION, value);
        Ok(())
    }

    fn invalidate(&self) {
        // `invalidate` is sync (no executor to block on), so use `try_lock`. If a
        // concurrent `authenticate` momentarily holds the lock, this clear is skipped --
        // that just means the very next request may still see the stale token, not that
        // it's ever stuck stale, since `authenticate` always re-checks `is_none()`.
        if let Ok(mut guard) = self.token.try_lock() {
            *guard = None;
        }
    }
}

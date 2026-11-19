use json_rpc_2_oauth2::{AccessTokenClaims, AuthError, AuthPlugin, JsonRpcAuthServer, RpcError};
use serde_json::Value;

#[derive(Clone)]
pub struct RpcAuthContext<S> {
    plugin: AuthPlugin<S>,
}

impl<S> RpcAuthContext<S> {
    pub fn new(plugin: AuthPlugin<S>) -> Self {
        Self { plugin }
    }

    pub fn authorize(
        &self,
        params: Value,
        permissions: &[&str],
    ) -> Result<AuthorizedCoopRequest, RpcError>
    where
        S: JsonRpcAuthServer<StorageError = AuthError> + Clone + 'static,
    {
        let request = self.plugin.authorize(params, permissions)?;
        Ok(AuthorizedCoopRequest {
            claims: request.claims,
            params: request.params,
        })
    }
}

pub struct AuthorizedCoopRequest {
    pub claims: AccessTokenClaims,
    pub params: Value,
}

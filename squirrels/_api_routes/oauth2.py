from fastapi import FastAPI, Depends, Request, Query, Response, APIRouter, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Annotated, cast

from .base import RouteBase
from .._schemas.auth_models import (
    ClientRegistrationRequest, ClientUpdateRequest, ClientRegistrationResponse, ClientDetailsResponse, ClientUpdateResponse, 
    TokenResponse, OAuthServerMetadata
)
from .._exceptions import InvalidInputError
from .. import _utils as u


class OAuth2Routes(RouteBase):
    """OAuth2 routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
    
    def serve_login_page(self, auth_path: str, request: Request, client_id: str) -> HTMLResponse:
        """Helper function to serve the login page with optional error message"""
        # Get client information for display
        client_details = self.authenticator.get_oauth_client_details(client_id)
        client_name = client_details.client_name if client_details else None
        project_name = self.manifest_cfg.project_variables.label
        
        # Get available login providers
        providers = []
        for provider in self.authenticator.auth_providers:
            provider_login_url = f"{auth_path}/providers/{provider.name}/login"
            providers.append({
                "name": provider.name,
                "label": provider.label,
                "icon": provider.icon,
                "login_url": provider_login_url
            })
        
        # Template context
        context = {
            "request": request,
            "project_name": project_name,
            "client_name": client_name,
            "providers": providers,
            "login_url": f"{auth_path}/login",
            "return_url": str(request.url),
        }
        
        return HTMLResponse(
            content=self.templates.get_template("oauth_login.html").render(context),
            status_code=200
        )
    
    def setup_routes(self, app: FastAPI, squirrels_version_path: str) -> None:
        """Setup all OAuth2 routes"""

        auth_path = squirrels_version_path + "/auth"
        router_path = auth_path + "/oauth2"
        router = APIRouter(prefix=router_path)
        
        # Create user models
        class UserInfoModel(self.UserInfoModel):
            username: str

        # Authorization dependency for client management
        get_client_token = HTTPBearer(auto_error=False)
        
        async def validate_client_registration_token(
            client_id: str, auth: HTTPAuthorizationCredentials = Depends(get_client_token),
        ) -> None:
            """Validate Bearer token for client management operations"""

            if not auth or not auth.scheme == "Bearer":
                raise InvalidInputError(401, "invalid_client", 
                    "Missing or invalid authorization header. Use 'Authorization: Bearer <registration_access_token>'"
                )
            
            token = auth.credentials
            is_valid = self.authenticator.validate_registration_access_token(client_id, token)
            if not is_valid:
                raise InvalidInputError(401, "invalid_token", "Invalid registration access token for this client")
        
        def validate_oauth_client_credentials(client_id: str | None, client_secret: str | None) -> str:
            """
            Validate OAuth client credentials from form data or Authorization header.
            Returns the validated client_id.
            """
            
            # Validate client credentials
            if not client_id or not client_secret or not self.authenticator.validate_client_credentials(client_id, client_secret):
                raise InvalidInputError(400, "invalid_client", "Invalid client credentials")
            
            return cast(str, client_id)
        
        # Client Registration Endpoint
        client_management_path = '/client/{client_id}'
        
        @router.post("/register", description="Register a new OAuth client", tags=["OAuth2"])
        async def register_oauth_client(request: ClientRegistrationRequest) -> ClientRegistrationResponse:
            """Register a new OAuth client and return client credentials"""
            
            # Register the client using the authenticator
            client_registration_response = self.authenticator.register_oauth_client(
                request, client_management_path_format=router_path+client_management_path
            )

            return client_registration_response
            
        # Client Management Endpoints
        @router.get(client_management_path, description="Get OAuth client registration details", tags=["OAuth2"])
        async def get_oauth_client(
            client_id: str, _: Annotated[None, Depends(validate_client_registration_token)]
        ) -> ClientDetailsResponse:
            """Get OAuth client registration details"""

            client_details = self.authenticator.get_oauth_client_details(client_id)
            
            return client_details
        
        @router.put(client_management_path, description="Update OAuth client registration", tags=["OAuth2"])
        async def update_oauth_client(
            client_id: str, request: ClientUpdateRequest, _: Annotated[None, Depends(validate_client_registration_token)]
        ) -> ClientUpdateResponse:
            """Update OAuth client registration and rotate access token"""

            # Update the client and get new registration access token
            client_details = self.authenticator.update_oauth_client_with_token_rotation(client_id, request)
            
            return client_details
        
        @router.delete(client_management_path, description="Revoke OAuth client registration", tags=["OAuth2"], responses={
            204: { "description": "OAuth client registration revoked successfully" }
        })
        async def revoke_oauth_client(
            client_id: str, _: Annotated[None, Depends(validate_client_registration_token)]
        ) -> Response:
            """Revoke (deactivate) OAuth client registration"""

            self.authenticator.revoke_oauth_client(client_id)
            return Response(status_code=204)
            
        # Authorization Endpoint
        @router.get("/authorize", description="OAuth 2.1 Authorization Endpoint", tags=["OAuth2"], response_model=None)
        async def authorize_endpoint(
            request: Request,
            response_type: str = Query(default="code", description="OAuth response type"),
            client_id: str = Query(..., description="OAuth client identifier"),
            redirect_uri: str = Query(..., description="URI to redirect after authorization"),
            scope: str = Query(default="read", description="Requested scope"),
            state: str | None = Query(default=None, description="State parameter for CSRF protection"),
            code_challenge: str = Query(..., description="PKCE code challenge (required)"),
            code_challenge_method: str = Query(default="S256", description="PKCE code challenge method"),
            user: UserInfoModel | None = Depends(self.get_current_user)
        ):
            """OAuth 2.1 authorization endpoint for initiating authorization code flow"""
            
            try:
                # Validate response_type
                if response_type != "code":
                    raise InvalidInputError(400, "unsupported_response_type", "Only 'code' response type is supported")
                
                # Check if user is authenticated
                if user is None:
                    # User is not authenticated - serve login page
                    return self.serve_login_page(auth_path, request, client_id)
                
                # TODO: Serve a page with an "authorize" button even if user is already authenticated
                # Ex. if not request.session.get("authorization_approved"), redirect to a page with button that submits to "/approve-authorization"
                
                # User is authenticated - generate authorization code
                authorization_code = self.authenticator.create_authorization_code(
                    client_id=client_id,
                    username=user.username,
                    redirect_uri=redirect_uri,
                    scope=scope,
                    code_challenge=code_challenge,
                    code_challenge_method=code_challenge_method
                )
                
                # Redirect back to client with authorization code
                success_params = f"?code={authorization_code}"
                if state:
                    success_params += f"&state={state}"
                
                return RedirectResponse(url=f"{redirect_uri}{success_params}")
                
            except InvalidInputError as e:
                if e.error == "invalid_request":
                    error_params = f"?error={e.error}&error_description={e.error_description.replace(' ', '+')}"
                    if state:
                        error_params += f"&state={state}"
                    return RedirectResponse(url=f"{redirect_uri}{error_params}")
                else:
                    raise e

        # Token Endpoint
        @router.post("/token", description="OAuth 2.1 Token Endpoint", tags=["OAuth2"])
        async def token_endpoint(
            grant_type: str = Form(...),
            code: str | None = Form(default=None),
            redirect_uri: str | None = Form(default=None),
            code_verifier: str | None = Form(default=None),
            refresh_token: str | None = Form(default=None),
            client_id: str | None = Form(default=None),
            client_secret: str | None = Form(default=None)
        ) -> TokenResponse:
            """OAuth 2.1 token endpoint for exchanging authorization code or refresh token for access token"""
            
            # Validate client credentials
            auth_client_id = validate_oauth_client_credentials(client_id, client_secret)
            
            # Get token expiry configuration
            expiry_mins = self._get_access_token_expiry_minutes()
            
            if grant_type == "authorization_code":
                # Validate required parameters for authorization code flow
                if not all([code, redirect_uri, code_verifier, auth_client_id]):
                    raise InvalidInputError(400, "invalid_request", "Missing required parameters for authorization_code grant")
                
                # Type casts since we validated above
                code = cast(str, code)
                redirect_uri = cast(str, redirect_uri)
                code_verifier = cast(str, code_verifier)
                auth_client_id = cast(str, auth_client_id)
                
                # Exchange authorization code for tokens
                token_response = self.authenticator.exchange_authorization_code(
                    code=code,
                    client_id=auth_client_id,
                    redirect_uri=redirect_uri,
                    code_verifier=code_verifier,
                    access_token_expiry_minutes=expiry_mins
                )
                
                return token_response
                
            elif grant_type == "refresh_token":
                # Validate required parameters for refresh token flow
                if not all([refresh_token, auth_client_id]):
                    raise InvalidInputError(400, "invalid_request", "Missing required parameters for refresh_token grant")
                
                # Type casts since we validated above
                refresh_token = cast(str, refresh_token)
                auth_client_id = cast(str, auth_client_id)
                
                # Refresh access token
                token_response = self.authenticator.refresh_oauth_access_token(
                    refresh_token=refresh_token,
                    client_id=auth_client_id,
                    access_token_expiry_minutes=expiry_mins
                )
                
                return token_response
            
            else:
                raise InvalidInputError(400, "unsupported_grant_type", f"Grant type '{grant_type}' is not supported")

        # Token Revocation Endpoint
        @router.post("/token/revoke", description="OAuth 2.1 Token Revocation Endpoint", tags=["OAuth2"])
        async def revoke_endpoint(
            token: str = Form(..., description="The token to be revoked"),
            token_type_hint: str | None = Form(default=None, description="Hint about the type of token being revoked"),
            client_id: str | None = Form(default=None),
            client_secret: str | None = Form(default=None)
        ) -> Response:
            """OAuth 2.1 token revocation endpoint for revoking refresh tokens"""
            
            # Validate client credentials
            auth_client_id = validate_oauth_client_credentials(client_id, client_secret)
            
            # Revoke the token (per RFC 7009, always return 200 regardless of token validity)
            try:
                self.authenticator.revoke_oauth_token(auth_client_id, token, token_type_hint)
            except InvalidInputError:
                # Per OAuth spec, revocation endpoint should return 200 even for invalid tokens
                pass
            
            return Response(status_code=200)

        # Authorization Server Metadata Endpoint (well-known endpoint)
        @app.get("/.well-known/oauth-authorization-server", tags=["OAuth2"], description="OAuth 2.1 Authorization Server Metadata")
        async def authorization_server_metadata(request: Request) -> OAuthServerMetadata:
            """OAuth 2.1 Authorization Server Metadata endpoint (RFC 8414)"""
            
            # Get the base URL from the request
            scheme = u.get_scheme(request.url.hostname)
            base_url = scheme + "://" + request.url.netloc
            
            return OAuthServerMetadata(
                issuer=base_url,
                authorization_endpoint=f"{base_url}{router_path}/authorize",
                token_endpoint=f"{base_url}{router_path}/token",
                revocation_endpoint=f"{base_url}{router_path}/token/revoke",
                registration_endpoint=f"{base_url}{router_path}/register",
                scopes_supported=["read"],
                response_types_supported=["code"],
                grant_types_supported=["authorization_code", "refresh_token"],
                token_endpoint_auth_methods_supported=["client_secret_post"],
                code_challenge_methods_supported=["S256"]
            )

        app.include_router(router)

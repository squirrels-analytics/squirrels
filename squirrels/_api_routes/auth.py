"""
Authentication and user management routes
"""
from typing import Annotated, Literal
from fastapi import FastAPI, Depends, Request, Response, status, Form, APIRouter
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel, Field
from authlib.integrations.starlette_client import OAuth

from .._schemas import response_models as rm
from .._exceptions import InvalidInputError
from .._auth import BaseUser
from .base import RouteBase


class AuthRoutes(RouteBase):
    """Authentication and user management routes"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project, no_cache: bool = False):
        super().__init__(get_bearer_token, project, no_cache)
        
    def setup_routes(self, app: FastAPI, squirrels_version_path: str) -> None:
        """Setup all authentication routes"""

        auth_path = squirrels_version_path + "/auth"
        auth_router = APIRouter(prefix=auth_path)
        user_management_router = APIRouter(prefix=auth_path + "/user-management")
        
        # Get expiry configuration
        expiry_mins = self._get_access_token_expiry_minutes()
        
        # Create user models
        class UpdateUserModel(self.UserModel):
            access_level: Literal["admin", "member"] # Cannot be "guest"

        class UserInfoModel(UpdateUserModel):
            username: str

        class AddUserModel(UserInfoModel):
            password: str
        
        # Setup OAuth2 login providers
        oauth = OAuth()

        for provider in self.authenticator.auth_providers:
            oauth.register(
                name=provider.name,
                server_metadata_url=provider.provider_configs.server_metadata_url,
                client_id=provider.provider_configs.client_id,
                client_secret=provider.provider_configs.client_secret,
                client_kwargs=provider.provider_configs.client_kwargs
            )

        # User info endpoint
        @auth_router.get("/userinfo", description="Get the authenticated user's fields", tags=["Authentication"])
        async def get_userinfo(user: UserInfoModel = Depends(self.get_current_user)) -> UserInfoModel:
            if user.access_level == "guest":
                raise InvalidInputError(401, "invalid_authorization_token", "Invalid authorization token")
            return user

        # Login helper
        def login_helper(
            request: Request, user: BaseUser, redirect_url: str | None, *, 
            redirect_status_code: int = status.HTTP_307_TEMPORARY_REDIRECT
        ):
            access_token, expiry = self.authenticator.create_access_token(user, expiry_minutes=expiry_mins)
            request.session["access_token"] = access_token
            request.session["access_token_expiry"] = expiry.timestamp()
            return RedirectResponse(url=redirect_url, status_code=redirect_status_code) if redirect_url else user

        # Login endpoints
        @auth_router.post("/login", tags=["Authentication"], description="Authenticate with username and password. Returns user information if no redirect_url is provided, otherwise redirects to the specified URL.", responses={
            200: {"model": UserInfoModel, "description": "Login successful, returns user information"},
            302: {"description": "Redirect if redirect URL parameter is specified"},
        })
        async def login(request: Request, username: Annotated[str, Form()], password: Annotated[str, Form()], redirect_url: str | None = None):
            if self.manifest_cfg.authentication.type.value == "external":
                raise InvalidInputError(403, "forbidden_login", "Username/password login is disabled when authentication.type is 'external'")
            user = self.authenticator.get_user(username, password)
            return login_helper(request, user, redirect_url, redirect_status_code=status.HTTP_302_FOUND)
        
        @auth_router.get("/login", tags=["Authentication"], description="Authenticate with an existing API key or session token. Returns user information if no redirect_url is provided, otherwise redirects to the specified URL.", responses={
            200: {"model": UserInfoModel, "description": "Login successful, returns user information"},
            307: {"description": "Redirect if redirect URL parameter is specified"},
        })
        async def login_with_api_key(
            request: Request, redirect_url: str | None = None, user: UserInfoModel | None = Depends(self.get_current_user)
        ):
            if user is None:
                raise InvalidInputError(401, "invalid_authorization_token", "Invalid authorization token")
            return login_helper(request, user, redirect_url)
        
        # Provider authentication endpoints
        providers_path = '/providers'
        provider_login_path = '/providers/{provider_name}/login'
        provider_callback_path = '/providers/{provider_name}/callback'

        @auth_router.get(providers_path, tags=["Authentication"])
        async def get_providers(request: Request) -> list[rm.ProviderResponse]:
            """Get list of available authentication providers"""
            return [
                rm.ProviderResponse(
                    name=provider.name,
                    label=provider.label,
                    icon=provider.icon,
                    login_url=str(request.url_for('provider_login', provider_name=provider.name))
                )
                for provider in self.authenticator.auth_providers
            ]

        @auth_router.get(provider_login_path, tags=["Authentication"])
        async def provider_login(request: Request, provider_name: str, redirect_url: str | None = None) -> RedirectResponse:
            """Redirect to the login URL for the OAuth provider"""
            client = oauth.create_client(provider_name)
            if client is None:
                raise InvalidInputError(status_code=404, error="provider_not_found", error_description=f"Provider {provider_name} not found or configured.")

            callback_uri = str(request.url_for('provider_callback', provider_name=provider_name))
            request.session["redirect_url"] = redirect_url

            return await client.authorize_redirect(request, callback_uri)

        @auth_router.get(provider_callback_path, tags=["Authentication"], responses={
            200: {"model": UserInfoModel, "description": "Login successful, returns user information"},
            302: {"description": "Redirect if redirect_url is in session"},
        })
        async def provider_callback(request: Request, provider_name: str):
            """Handle OAuth callback from provider"""
            client = oauth.create_client(provider_name)
            if client is None:
                raise InvalidInputError(status_code=404, error="provider_not_found", error_description=f"Provider {provider_name} not found or configured.")

            try:
                token = await client.authorize_access_token(request)
            except Exception as e:
                raise InvalidInputError(status_code=400, error="provider_authorization_failed", error_description=f"Could not authorize with provider for access token: {str(e)}")
            
            user_info: dict = {}
            if token:
                if 'userinfo' in token:
                    user_info = token['userinfo']
                elif 'id_token' in token and isinstance(token['id_token'], dict) and 'sub' in token['id_token']:
                    user_info = token['id_token']
                else:
                    raise InvalidInputError(status_code=400, error="invalid_provider_user_info", error_description=f"User information not found in token for {provider_name}")

            user = self.authenticator.create_or_get_user_from_provider(provider_name, user_info)
            access_token, expiry = self.authenticator.create_access_token(user, expiry_minutes=expiry_mins)
            request.session["access_token"] = access_token
            request.session["access_token_expiry"] = expiry.timestamp()

            redirect_url = request.session.pop("redirect_url", None)
            return RedirectResponse(url=redirect_url) if redirect_url else user

        # Logout endpoint
        logout_path = '/logout'
        
        @auth_router.get(logout_path, tags=["Authentication"], responses={
            200: {"description": "Logout successful"},
            302: {"description": "Redirect if redirect URL parameter is specified"},
        })
        async def logout(request: Request, redirect_url: str | None = None):
            """Logout the current user, and redirect to the specified URL if provided"""
            request.session.pop("access_token", None)
            request.session.pop("access_token_expiry", None)
            if redirect_url:
                return RedirectResponse(url=redirect_url)
        
        # Change password endpoint
        change_password_path = '/password'

        class ChangePasswordRequest(BaseModel):
            old_password: str
            new_password: str

        @auth_router.put(change_password_path, description="Change the password for the current user", tags=["Authentication"])
        async def change_password(request: ChangePasswordRequest, user: UserInfoModel | None = Depends(self.get_current_user)) -> None:
            if user is None:
                raise InvalidInputError(401, "invalid_authorization_token", "Invalid authorization token")
            self.authenticator.change_password(user.username, request.old_password, request.new_password)

        # API Key endpoints
        api_key_path = '/api-key'

        class ApiKeyRequestBody(BaseModel):
            title: str = Field(description=f"The title of the API key")
            expiry_minutes: int | None = Field(
                default=None, 
                description=f"The number of minutes the API key is valid for (or valid indefinitely if not provided)."
            )

        @auth_router.post(api_key_path, description="Create a new API key for the user", tags=["Authentication"])
        async def create_api_key(body: ApiKeyRequestBody, user: UserInfoModel | None = Depends(self.get_current_user)) -> rm.ApiKeyResponse:
            if user is None:
                raise InvalidInputError(401, "invalid_authorization_token", "Invalid authorization token")
            
            api_key, _ = self.authenticator.create_access_token(user, expiry_minutes=body.expiry_minutes, title=body.title)
            return rm.ApiKeyResponse(api_key=api_key)
        
        @auth_router.get(api_key_path, description="Get all API keys with title for the current user", tags=["Authentication"])
        async def get_all_api_keys(user: UserInfoModel | None = Depends(self.get_current_user)):
            if user is None:
                raise InvalidInputError(401, "invalid_authorization_token", "Invalid authorization token")
            return self.authenticator.get_all_api_keys(user.username)
        
        revoke_api_key_path = '/api-key/{api_key_id}'

        @auth_router.delete(revoke_api_key_path, description="Revoke an API key", tags=["Authentication"], responses={
            204: { "description": "API key revoked successfully" }
        })
        async def revoke_api_key(api_key_id: str, user: UserInfoModel | None = Depends(self.get_current_user)) -> Response:
            if user is None:
                raise InvalidInputError(401, "invalid_authorization_token", "Invalid authorization token")
            self.authenticator.revoke_api_key(user.username, api_key_id)
            return Response(status_code=204)

        # User management endpoints (disabled if external auth only)
        if self.manifest_cfg.authentication.type.value == "managed":
            user_fields_path = '/user-fields'

            @user_management_router.get(user_fields_path, description="Get details of the user fields", tags=["User Management"])
            async def get_user_fields():
                return self.authenticator.user_fields
            
            add_user_path = '/users'

            @user_management_router.post(add_user_path, description="Add a new user by providing details for username, password, and user fields", tags=["User Management"])
            async def add_user(
                new_user: AddUserModel, user: UserInfoModel = Depends(self.get_current_user)
            ) -> None:
                if user.access_level != "admin":
                    raise InvalidInputError(403, "unauthorized_to_add_user", "Current user cannot add new users")
                self.authenticator.add_user(new_user.username, new_user.model_dump(mode='json', exclude={"username"}))

            update_user_path = '/users/{username}'

            @user_management_router.put(update_user_path, description="Update the user of the given username given the new user details", tags=["User Management"])
            async def update_user(
                username: str, updated_user: UpdateUserModel, user: UserInfoModel = Depends(self.get_current_user)
            ) -> None:
                if user.access_level != "admin":
                    raise InvalidInputError(403, "unauthorized_to_update_user", "Current user cannot update users")
                self.authenticator.add_user(username, updated_user.model_dump(mode='json'), update_user=True)

            list_users_path = '/users'

            @user_management_router.get(list_users_path, tags=["User Management"])
            async def list_all_users():
                return self.authenticator.get_all_users()
            
            delete_user_path = '/users/{username}'

            @user_management_router.delete(delete_user_path, tags=["User Management"], responses={
                204: { "description": "User deleted successfully" }
            })
            async def delete_user(username: str, user: UserInfoModel = Depends(self.get_current_user)) -> Response:
                if user.access_level != "admin":
                    raise InvalidInputError(403, "unauthorized_to_delete_user", "Current user cannot delete users")
                if username == user.username:
                    raise InvalidInputError(403, "cannot_delete_own_user", "Cannot delete your own user")
                self.authenticator.delete_user(username)
                return Response(status_code=204)

        app.include_router(auth_router)
        app.include_router(user_management_router)

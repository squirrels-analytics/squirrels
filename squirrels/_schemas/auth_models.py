from typing import Callable, Any, Literal
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_serializer


class BaseUser(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    username: str
    access_level: Literal["admin", "member", "guest"] = "guest"
    
    @classmethod
    def dropped_columns(cls):
        return []
    
    def __hash__(self):
        return hash(self.username)
    
    def __str__(self):
        return self.username


class ApiKey(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    title: str
    username: str
    created_at: datetime
    expires_at: datetime
    
    @field_serializer('created_at', 'expires_at')
    def serialize_datetime(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class UserField(BaseModel):
    name: str
    type: str
    nullable: bool
    enum: list[str] | None
    default: Any | None


class ProviderConfigs(BaseModel):
    client_id: str
    client_secret: str
    server_metadata_url: str
    client_kwargs: dict = Field(default_factory=dict)
    get_user: Callable[[dict], BaseUser]


class AuthProvider(BaseModel):
    name: str
    label: str
    icon: str
    provider_configs: ProviderConfigs


# OAuth 2.1 Models

class OAuthClientModel(BaseModel):
    """OAuth client details"""
    model_config = ConfigDict(from_attributes=True)
    client_id: str
    client_name: str
    redirect_uris: list[str]
    scope: str
    grant_types: list[str]
    response_types: list[str]
    created_at: datetime
    is_active: bool
    
    @field_serializer('created_at')
    def serialize_datetime(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class ClientRegistrationRequest(BaseModel):
    """Request model for OAuth client registration"""
    client_name: str = Field(description="Human-readable name for the OAuth client")
    redirect_uris: list[str] = Field(description="List of allowed redirect URIs for the client")
    scope: str = Field(default="read", description="Default scope for the client")
    grant_types: list[str] = Field(default=["authorization_code", "refresh_token"], description="Allowed grant types")
    response_types: list[str] = Field(default=["code"], description="Allowed response types")


class ClientUpdateRequest(BaseModel):
    """Request model for OAuth client update"""
    client_name: str | None = Field(default=None, description="Human-readable name for the OAuth client")
    redirect_uris: list[str] | None = Field(default=None, description="List of allowed redirect URIs for the client")
    scope: str | None = Field(default=None, description="Default scope for the client")
    grant_types: list[str] | None = Field(default=None, description="Allowed grant types")
    response_types: list[str] | None = Field(default=None, description="Allowed response types")
    is_active: bool | None = Field(default=None, description="Whether the client is active")


class ClientDetailsResponse(BaseModel):
    """Response model for OAuth client details (without client_secret)"""
    client_id: str = Field(description="Client ID")
    client_name: str = Field(description="Client name")
    redirect_uris: list[str] = Field(description="Registered redirect URIs")
    scope: str = Field(description="Default scope")
    grant_types: list[str] = Field(description="Allowed grant types")
    response_types: list[str] = Field(description="Allowed response types")
    created_at: datetime = Field(description="Registration timestamp")
    is_active: bool = Field(description="Whether the client is active")
    
    @field_serializer('created_at')
    def serialize_datetime(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class ClientUpdateResponse(ClientDetailsResponse):
    """Response model for OAuth client update"""
    registration_access_token: str | None = Field(default=None, description="Token for managing this client registration (store securely)")


class ClientRegistrationResponse(ClientUpdateResponse):
    """Response model for OAuth client registration"""
    client_secret: str = Field(description="Generated client secret (store securely)")
    registration_client_uri: str | None = Field(default=None, description="URI for managing this client registration")


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str | None = None


class OAuthServerMetadata(BaseModel):
    """OAuth 2.1 Authorization Server Metadata (RFC 8414)"""
    issuer: str = Field(description="Authorization server's issuer identifier URL")
    authorization_endpoint: str = Field(description="URL of the authorization endpoint")
    token_endpoint: str = Field(description="URL of the token endpoint")
    revocation_endpoint: str = Field(description="URL of the token revocation endpoint")
    registration_endpoint: str = Field(description="URL of the client registration endpoint")
    scopes_supported: list[str] = Field(description="List of OAuth 2.1 scope values supported")
    response_types_supported: list[str] = Field(description="List of OAuth 2.1 response_type values supported")
    grant_types_supported: list[str] = Field(description="List of OAuth 2.1 grant type values supported")
    token_endpoint_auth_methods_supported: list[str] = Field(
        default=["client_secret_basic", "client_secret_post"], 
        description="List of client authentication methods supported by the token endpoint"
    )
    code_challenge_methods_supported: list[str] = Field(
        default=["S256"], 
        description="List of PKCE code challenge methods supported"
    )

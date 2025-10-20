import pytest
from sqlalchemy import create_engine
from enum import Enum
from passlib.context import CryptContext

from squirrels._auth import Authenticator, BaseUser, AuthProviderArgs
from squirrels._manifest import PermissionScope
from squirrels._exceptions import InvalidInputError
from squirrels._schemas.auth_models import ClientRegistrationRequest, ClientUpdateRequest
from squirrels import _utils as u, _constants as c

# Fast password context for testing (much faster than production bcrypt)
test_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__rounds=4)

# Test User model with custom fields
class RoleEnum(str, Enum):
    USER = "user"
    MODERATOR = "moderator"
    ADMIN = "admin"

class User(BaseUser):
    email: str = ""
    role: RoleEnum = RoleEnum.USER
    age: int = 18

@pytest.fixture(scope="session")
def env_vars():
    return {
        c.SQRL_SECRET_KEY: "test_secret_key",
        c.SQRL_SECRET_ADMIN_PASSWORD: "admin_password"
    }

@pytest.fixture(scope="function")
def auth(env_vars, monkeypatch):
    # Patch the password context to use faster hashing for tests
    monkeypatch.setattr("squirrels._auth.pwd_context", test_pwd_context)
    
    engine = create_engine("sqlite:///:memory:?check_same_thread=False")
    logger = u.Logger("")
    auth_args = AuthProviderArgs(project_path=".", _proj_vars={}, _env_vars=env_vars)
    auth_instance = Authenticator(logger, ".", auth_args, provider_functions=[], user_cls=User, sa_engine=engine)
    yield auth_instance
    auth_instance.close()

@pytest.fixture
def test_user_data():
    """Standard test user data to reduce duplication"""
    return {
        "password": "password123",
        "email": "test@example.com",
        "role": RoleEnum.MODERATOR,
        "age": 25
    }

def test_initialize_db(auth: Authenticator[User]):
    # Check if admin user was created
    users = auth.get_all_users()
    assert len(users) == 1
    admin = users[0]
    assert admin.username == "admin"
    assert admin.access_level == "admin"

def test_add_and_get_user(auth: Authenticator[User], test_user_data):
    # Add a new user
    auth.add_user("testuser", test_user_data)

    # Get the user with correct password
    user = auth.get_user("testuser", "password123")
    assert user.username == "testuser"
    assert user.email == "test@example.com"
    assert user.role == RoleEnum.MODERATOR
    assert user.age == 25
    assert user.access_level == "member"

    # Try getting user with wrong password
    with pytest.raises(InvalidInputError) as exc_info:
        auth.get_user("testuser", "wrongpassword")
    assert exc_info.value.error == "incorrect_username_or_password"

def test_change_password(auth: Authenticator[User]):
    # Add a user first
    auth.add_user("pwduser", {
        "password": "oldpassword",
        "email": "pwd@example.com"
    })

    # Change password
    auth.change_password("pwduser", "oldpassword", "newpassword")

    # Old password should fail
    with pytest.raises(InvalidInputError) as exc_info:
        auth.get_user("pwduser", "oldpassword")
    assert exc_info.value.error == "incorrect_username_or_password"

    # New password should work
    user = auth.get_user("pwduser", "newpassword")
    assert user.username == "pwduser"

def test_delete_user(auth: Authenticator[User]):
    # Add a user
    auth.add_user("deleteuser", {
        "password": "password123",
        "email": "delete@example.com"
    })

    # Verify user exists
    users_before = auth.get_all_users()
    assert any(u.username == "deleteuser" for u in users_before)

    # Delete user
    auth.delete_user("deleteuser")

    # Verify user is gone
    users_after = auth.get_all_users()
    assert not any(u.username == "deleteuser" for u in users_after)

def test_access_tokens(auth: Authenticator[User], test_user_data):
    # Add a user
    auth.add_user("tokenuser", test_user_data)

    # Get user
    user = auth.get_user("tokenuser", "password123")

    # Create access token
    token, _ = auth.create_access_token(user, 30, title="Test Token")
    
    # Verify token
    token_user = auth.get_user_from_token(token)
    assert token_user is not None
    assert token_user.username == "tokenuser"

    # Get all tokens
    tokens = auth.get_all_api_keys("tokenuser")
    assert len(tokens) == 1
    assert str(tokens[0].title) == "Test Token"
    assert str(tokens[0].username) == "tokenuser"

def test_permission_scopes(auth: Authenticator[User], test_user_data):
    guest_user = User(username="", access_level="guest")

    # Add a regular user
    auth.add_user("regular", test_user_data)
    regular_user = auth.get_user("regular", "password123")

    # Get admin user
    admin_user = auth.get_user("admin", "admin_password")

    # Test permission scopes
    assert auth.can_user_access_scope(guest_user, PermissionScope.PUBLIC) == True
    assert auth.can_user_access_scope(guest_user, PermissionScope.PROTECTED) == False
    assert auth.can_user_access_scope(guest_user, PermissionScope.PRIVATE) == False

    assert auth.can_user_access_scope(regular_user, PermissionScope.PUBLIC) == True
    assert auth.can_user_access_scope(regular_user, PermissionScope.PROTECTED) == True
    assert auth.can_user_access_scope(regular_user, PermissionScope.PRIVATE) == False

    assert auth.can_user_access_scope(admin_user, PermissionScope.PUBLIC) == True
    assert auth.can_user_access_scope(admin_user, PermissionScope.PROTECTED) == True
    assert auth.can_user_access_scope(admin_user, PermissionScope.PRIVATE) == True

def test_expired_token(auth: Authenticator[User], test_user_data):
    # Add a user
    auth.add_user("expireduser", test_user_data)

    # Get user
    user = auth.get_user("expireduser", "password123")

    # Create token that expires in -1 minutes (already expired)
    token, _ = auth.create_access_token(user, -1)

    # Verify token is invalid
    with pytest.raises(InvalidInputError) as exc_info:
        auth.get_user_from_token(token)
    assert exc_info.value.error == "invalid_authorization_token"


# OAuth Client Management Tests

@pytest.fixture
def sample_client_request():
    """Standard OAuth client registration request for testing"""
    return ClientRegistrationRequest(
        client_name="Test OAuth Client",
        redirect_uris=["https://example.com/callback", "https://app.example.com/oauth/callback"],
        scope="read",
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"]
    )

@pytest.fixture
def invalid_redirect_client_request():
    """OAuth client request with invalid redirect URIs"""
    return ClientRegistrationRequest(
        client_name="Invalid Client",
        redirect_uris=["http://insecure.com/callback", "invalid://bad-scheme"],
        scope="read"
    )

def test_register_oauth_client_basic(auth: Authenticator[User], sample_client_request):
    """Test basic OAuth client registration"""
    client_management_path = "/api/oauth/client/{client_id}"
    
    # Register the client
    response = auth.register_oauth_client(sample_client_request, client_management_path)
    
    # Verify response structure
    assert response.is_active == True
    assert response.created_at is not None
    
    # Verify sensitive fields are present
    assert response.client_id is not None
    assert len(response.client_id) > 0
    assert response.client_secret is not None
    assert len(response.client_secret) > 0
    assert response.registration_access_token is not None
    assert len(response.registration_access_token) > 0
    assert response.registration_client_uri == f"/api/oauth/client/{response.client_id}"

def test_register_oauth_client_invalid_redirect_uris(auth: Authenticator[User], invalid_redirect_client_request):
    """Test OAuth client registration with invalid redirect URIs"""
    
    # Should raise InvalidInputError for invalid redirect URIs
    with pytest.raises(InvalidInputError) as exc_info:
        auth.register_oauth_client(invalid_redirect_client_request, "")
    
    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "invalid_redirect_uri"

def test_get_oauth_client_details(auth: Authenticator[User], sample_client_request):
    """Test retrieving OAuth client details"""
    
    # Register a client first
    registration_response = auth.register_oauth_client(sample_client_request, "")
    client_id = registration_response.client_id
    
    # Get client details
    client_details = auth.get_oauth_client_details(client_id)
    
    # Verify details match registration (without sensitive info)
    assert client_details is not None
    assert client_details.client_id == client_id
    assert client_details.client_name == "Test OAuth Client"
    assert client_details.redirect_uris == ["https://example.com/callback", "https://app.example.com/oauth/callback"]
    assert client_details.scope == "read"
    assert client_details.grant_types == ["authorization_code", "refresh_token"]
    assert client_details.is_active == True
    
    # Sensitive fields should not be present
    assert not hasattr(client_details, 'client_secret')
    assert not hasattr(client_details, 'registration_access_token')

def test_get_oauth_client_details_nonexistent(auth: Authenticator[User]):
    """Test retrieving details for non-existent client"""
    
    with pytest.raises(InvalidInputError) as exc_info:
        auth.get_oauth_client_details("nonexistent_client_id")
    
    assert exc_info.value.status_code == 404
    assert exc_info.value.error == "invalid_client_id"

def test_validate_client_credentials(auth: Authenticator[User], sample_client_request):
    """Test OAuth client credential validation"""
    
    # Register a client
    response = auth.register_oauth_client(sample_client_request, "")
    
    # Valid credentials should return True
    is_valid = auth.validate_client_credentials(response.client_id, response.client_secret)
    assert is_valid
    
    # Invalid client_id should return False
    is_valid = auth.validate_client_credentials("invalid_id", response.client_secret)
    assert not is_valid
    
    # Invalid client_secret should return False
    is_valid = auth.validate_client_credentials(response.client_id, "invalid_secret")
    assert not is_valid

def test_validate_redirect_uri(auth: Authenticator[User], sample_client_request):
    """Test redirect URI validation for registered client"""
    
    # Register a client
    response = auth.register_oauth_client(sample_client_request, "")
    
    # Registered URIs should be valid
    assert auth.validate_redirect_uri(response.client_id, "https://example.com/callback")
    assert auth.validate_redirect_uri(response.client_id, "https://app.example.com/oauth/callback")
    
    # Non-registered URI should be invalid
    assert not auth.validate_redirect_uri(response.client_id, "https://different.com/callback")
    
    # Invalid client_id should return False
    assert not auth.validate_redirect_uri("invalid_id", "https://example.com/callback")

def test_validate_registration_access_token(auth: Authenticator[User], sample_client_request):
    """Test registration access token validation"""
    
    # Register a client with management enabled
    response = auth.register_oauth_client(sample_client_request, "/api/oauth/client/{client_id}")
    
    # Valid token should return True
    assert response.registration_access_token is not None
    assert auth.validate_registration_access_token(response.client_id, response.registration_access_token)
    
    # Invalid token should return False
    assert not auth.validate_registration_access_token(response.client_id, "invalid_token")
    
    # Invalid client_id should return False
    assert not auth.validate_registration_access_token("invalid_id", response.registration_access_token)

def test_update_oauth_client_with_token_rotation(auth: Authenticator[User], sample_client_request):
    """Test OAuth client update with registration token rotation"""
    
    # Register a client
    response = auth.register_oauth_client(sample_client_request, "/api/oauth/client/{client_id}")
    original_token = response.registration_access_token
    assert original_token is not None
    
    # Update client details
    update_request = ClientUpdateRequest(
        client_name="Updated Test Client",
        scope="read",
        redirect_uris=["https://newapp.example.com/callback"]
    )
    
    update_response = auth.update_oauth_client_with_token_rotation(response.client_id, update_request)
    
    # Verify updated fields
    assert update_response.client_name == "Updated Test Client"
    assert update_response.scope == "read"
    assert update_response.redirect_uris == ["https://newapp.example.com/callback"]
    
    # Verify token was rotated
    assert update_response.registration_access_token is not None
    assert update_response.registration_access_token != original_token
    
    # Original token should no longer be valid
    assert not auth.validate_registration_access_token(response.client_id, original_token)
    
    # New token should be valid
    assert auth.validate_registration_access_token(response.client_id, update_response.registration_access_token)

def test_update_oauth_client_invalid_redirect_uris(auth: Authenticator[User], sample_client_request):
    """Test OAuth client update with invalid redirect URIs"""
    
    # Register a client
    response = auth.register_oauth_client(sample_client_request, "/api/oauth/client/{client_id}")
    
    # Try to update with invalid redirect URIs
    with pytest.raises(InvalidInputError) as exc_info:
        auth.update_oauth_client_with_token_rotation(
            response.client_id, ClientUpdateRequest(redirect_uris=["http://insecure.com/callback"])
        )
    
    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "invalid_redirect_uri"

def test_update_oauth_client_nonexistent(auth: Authenticator[User]):
    """Test updating non-existent OAuth client"""
    
    with pytest.raises(InvalidInputError) as exc_info:
        auth.update_oauth_client_with_token_rotation(
            "nonexistent_client_id", ClientUpdateRequest(client_name="Updated Name")
        )
    
    assert exc_info.value.status_code == 404
    assert exc_info.value.error == "invalid_client_id"

def test_revoke_oauth_client(auth: Authenticator[User], sample_client_request):
    """Test OAuth client revocation"""
    
    # Register a client
    response = auth.register_oauth_client(sample_client_request, "")
    
    # Verify client is initially active
    client_details = auth.get_oauth_client_details(response.client_id)
    assert client_details is not None
    assert client_details.is_active
    
    # Revoke the client
    auth.revoke_oauth_client(response.client_id)
    
    # Verify client is deactivated
    with pytest.raises(InvalidInputError) as exc_info:
        auth.get_oauth_client_details(response.client_id)
    
    assert exc_info.value.status_code == 404
    assert exc_info.value.error == "invalid_client_id"
    
    # Client credentials should no longer be valid
    assert not auth.validate_client_credentials(response.client_id, response.client_secret)

def test_revoke_oauth_client_nonexistent(auth: Authenticator[User]):
    """Test revoking non-existent OAuth client"""
    
    with pytest.raises(InvalidInputError) as exc_info:
        auth.revoke_oauth_client("nonexistent_client_id")
    
    assert exc_info.value.status_code == 404
    assert exc_info.value.error == "client_not_found"

def test_validate_redirect_uri_format(auth: Authenticator[User]):
    """Test redirect URI format validation"""
    
    # Valid HTTPS URIs
    assert auth._validate_redirect_uri_format("https://example.com/callback")
    assert auth._validate_redirect_uri_format("https://app.example.com/oauth/callback")
    
    # Valid localhost HTTP URIs
    assert auth._validate_redirect_uri_format("http://localhost:3000/callback")
    assert auth._validate_redirect_uri_format("http://127.0.0.1:8080/callback")
    
    # Valid custom schemes (for mobile apps)
    assert auth._validate_redirect_uri_format("myapp://oauth/callback")
    assert auth._validate_redirect_uri_format("com.example.app://auth")
    
    # Invalid URIs with fragments
    assert not auth._validate_redirect_uri_format("https://example.com/callback#fragment")
    
    # Invalid HTTP URIs (non-localhost)
    assert not auth._validate_redirect_uri_format("http://example.com/callback")
    
    # Invalid schemes
    assert not auth._validate_redirect_uri_format("invalid-scheme")
    assert not auth._validate_redirect_uri_format("")

def test_oauth_client_end_to_end(auth: Authenticator[User], sample_client_request):
    """Test complete OAuth client lifecycle"""
    
    # 1. Register client
    registration = auth.register_oauth_client(sample_client_request, "/api/oauth/client/{client_id}")
    assert registration.is_active
    
    # 2. Validate credentials
    assert auth.validate_client_credentials(registration.client_id, registration.client_secret)
    
    # 3. Validate redirect URI
    assert auth.validate_redirect_uri(registration.client_id, "https://example.com/callback")
    
    # 4. Get client details
    details = auth.get_oauth_client_details(registration.client_id)
    assert details is not None
    assert details.client_name == "Test OAuth Client"
    
    # 5. Update client
    update_response = auth.update_oauth_client_with_token_rotation(
        registration.client_id, ClientUpdateRequest(client_name="Updated Client Name")
    )
    assert update_response.client_name == "Updated Client Name"
    
    # 6. Verify update persisted
    updated_details = auth.get_oauth_client_details(registration.client_id)
    assert updated_details is not None
    assert updated_details.client_name == "Updated Client Name"
    
    # 7. Revoke client
    auth.revoke_oauth_client(registration.client_id)
    
    # 8. Verify revocation
    with pytest.raises(InvalidInputError) as exc_info:
        auth.get_oauth_client_details(registration.client_id)
    
    assert exc_info.value.status_code == 404
    assert exc_info.value.error == "invalid_client_id"


def test_revoke_oauth_token_basic(auth: Authenticator[User], sample_client_request):
    """Test basic OAuth token revocation"""
    
    # Register a client and user
    auth.add_user("oauth_user", {"password": "password123", "email": "oauth@example.com"})
    registration = auth.register_oauth_client(sample_client_request, "")
    
    # Create authorization code
    auth_code = auth.create_authorization_code(
        client_id=registration.client_id,
        username="oauth_user",
        redirect_uri="https://example.com/callback",
        scope="read",
        code_challenge="E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        code_challenge_method="S256"
    )
    
    # Exchange for tokens
    token_response = auth.exchange_authorization_code(
        code=auth_code,
        client_id=registration.client_id,
        redirect_uri="https://example.com/callback",
        code_verifier="dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
        access_token_expiry_minutes=60
    )
    
    # Revoke the refresh token
    assert token_response.refresh_token is not None
    auth.revoke_oauth_token(registration.client_id, token_response.refresh_token, "refresh_token")
    
    # Try to use the revoked refresh token - should fail
    with pytest.raises(InvalidInputError) as exc_info:
        auth.refresh_oauth_access_token(
            refresh_token=token_response.refresh_token,
            client_id=registration.client_id,
            access_token_expiry_minutes=60
        )
    
    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "invalid_grant"


def test_revoke_oauth_token_invalid_client(auth: Authenticator[User]):
    """Test revoking token with invalid client credentials"""
    
    with pytest.raises(InvalidInputError) as exc_info:
        auth.revoke_oauth_token("invalid_client_id", "some_token", "refresh_token")
    
    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "invalid_client"


def test_revoke_oauth_token_nonexistent_token(auth: Authenticator[User], sample_client_request):
    """Test revoking non-existent token (should succeed per OAuth spec)"""
    
    # Register a client
    registration = auth.register_oauth_client(sample_client_request, "")
    
    # Try to revoke non-existent token - should not raise error
    auth.revoke_oauth_token(registration.client_id, "nonexistent_token", "refresh_token")


def test_refresh_token_after_revocation(auth: Authenticator[User], sample_client_request):
    """Test that refresh token flow properly handles token revocation"""
    
    # Register a client and user
    auth.add_user("oauth_user", {"password": "password123", "email": "oauth@example.com"})
    registration = auth.register_oauth_client(sample_client_request, "")
    
    # Create authorization code and exchange for tokens
    auth_code = auth.create_authorization_code(
        client_id=registration.client_id,
        username="oauth_user",
        redirect_uri="https://example.com/callback",
        scope="read",
        code_challenge="E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
        code_challenge_method="S256"
    )
    
    token_response = auth.exchange_authorization_code(
        code=auth_code,
        client_id=registration.client_id,
        redirect_uri="https://example.com/callback",
        code_verifier="dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
        access_token_expiry_minutes=60
    )
    
    # Use refresh token to get new tokens
    assert token_response.refresh_token is not None
    new_token_response = auth.refresh_oauth_access_token(
        refresh_token=token_response.refresh_token,
        client_id=registration.client_id,
        access_token_expiry_minutes=60
    )
    
    # Original refresh token should now be revoked
    with pytest.raises(InvalidInputError) as exc_info:
        auth.refresh_oauth_access_token(
            refresh_token=token_response.refresh_token,
            client_id=registration.client_id,
            access_token_expiry_minutes=60
        )
    
    assert exc_info.value.status_code == 400
    assert exc_info.value.error == "invalid_grant"
    
    # New refresh token should work
    assert new_token_response.refresh_token is not None
    newer_token_response = auth.refresh_oauth_access_token(
        refresh_token=new_token_response.refresh_token,
        client_id=registration.client_id,
        access_token_expiry_minutes=60
    )
    
    assert newer_token_response.refresh_token != new_token_response.refresh_token
    
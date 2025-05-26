import pytest
from sqlalchemy import create_engine
from enum import Enum
from passlib.context import CryptContext

from squirrels._auth import Authenticator, BaseUser, AuthProviderArgs
from squirrels._manifest import PermissionScope
from squirrels._exceptions import InvalidInputErrorTmp
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
    assert admin.is_admin == True

def test_add_and_get_user(auth: Authenticator[User], test_user_data):
    # Add a new user
    auth.add_user("testuser", test_user_data)

    # Get the user with correct password
    user = auth.get_user("testuser", "password123")
    assert user.username == "testuser"
    assert user.email == "test@example.com"
    assert user.role == RoleEnum.MODERATOR
    assert user.age == 25
    assert user.is_admin == False

    # Try getting user with wrong password
    with pytest.raises(InvalidInputErrorTmp):
        auth.get_user("testuser", "wrongpassword")

def test_change_password(auth: Authenticator[User]):
    # Add a user first
    auth.add_user("pwduser", {
        "password": "oldpassword",
        "email": "pwd@example.com"
    })

    # Change password
    auth.change_password("pwduser", "oldpassword", "newpassword")

    # Old password should fail
    with pytest.raises(InvalidInputErrorTmp):
        auth.get_user("pwduser", "oldpassword")

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
    token = auth.create_access_token(user, 30, title="Test Token")
    
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
    # Add a regular user
    auth.add_user("regular", test_user_data)
    regular_user = auth.get_user("regular", "password123")

    # Get admin user
    admin_user = auth.get_user("admin", "admin_password")

    # Test permission scopes
    assert auth.can_user_access_scope(None, PermissionScope.PUBLIC) == True
    assert auth.can_user_access_scope(None, PermissionScope.PROTECTED) == False
    assert auth.can_user_access_scope(None, PermissionScope.PRIVATE) == False

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
    token = auth.create_access_token(user, -1)

    # Verify token is invalid
    with pytest.raises(InvalidInputErrorTmp):
        auth.get_user_from_token(token)
    
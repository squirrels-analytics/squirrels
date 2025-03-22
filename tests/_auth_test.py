import pytest
from sqlalchemy import create_engine
from enum import Enum

from squirrels._auth import Authenticator, BaseUser
from squirrels._manifest import PermissionScope
from squirrels._exceptions import InvalidInputError
from squirrels import _utils as u, _constants as c

# Test User model with custom fields
class RoleEnum(str, Enum):
    USER = "user"
    MODERATOR = "moderator"
    ADMIN = "admin"

class User(BaseUser):
    email: str = ""
    role: RoleEnum = RoleEnum.USER
    age: int = 18

@pytest.fixture(scope="module")
def env_vars():
    return {
        c.SQRL_SECRET_KEY: "test_secret_key",
        c.SQRL_SECRET_ADMIN_PASSWORD: "admin_password"
    }

@pytest.fixture(scope="function")
def auth(env_vars):
    engine = create_engine("sqlite://")
    logger = u.Logger("")
    auth = Authenticator(logger, ".", env_vars, sa_engine=engine, cls=User)
    yield auth
    auth.close()

def test_initialize_db(auth: Authenticator[User]):
    # Check if admin user was created
    users = auth.get_all_users()
    assert len(users) == 1
    admin = users[0]
    assert admin.username == "admin"
    assert admin.is_admin == True

def test_add_and_get_user(auth: Authenticator[User]):
    # Add a new user
    user_data = {
        "password": "password123",
        "email": "test@example.com",
        "role": RoleEnum.MODERATOR,
        "age": 25
    }
    auth.add_user("testuser", user_data)

    # Get the user with correct password
    user = auth.get_user("testuser", "password123")
    assert user.username == "testuser"
    assert user.email == "test@example.com"
    assert user.role == RoleEnum.MODERATOR
    assert user.age == 25
    assert user.is_admin == False

    # Try getting user with wrong password
    with pytest.raises(InvalidInputError):
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
    with pytest.raises(InvalidInputError):
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

def test_access_tokens(auth: Authenticator[User]):
    # Add a user
    auth.add_user("tokenuser", {
        "password": "password123",
        "email": "token@example.com"
    })

    # Get user
    user = auth.get_user("tokenuser", "password123")

    # Create access token
    token, _ = auth.create_access_token(user, 30, title="Test Token")
    
    # Verify token
    token_user = auth.get_user_from_token(token)
    assert token_user is not None
    assert token_user.username == "tokenuser"

    # Get all tokens
    tokens = auth.get_all_tokens("tokenuser")
    assert len(tokens) == 1
    assert str(tokens[0].title) == "Test Token"
    assert str(tokens[0].username) == "tokenuser"

def test_permission_scopes(auth: Authenticator[User]):
    # Add a regular user
    auth.add_user("regular", {
        "password": "password123",
        "email": "regular@example.com"
    })
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

def test_expired_token(auth: Authenticator[User]):
    # Add a user
    auth.add_user("expireduser", {
        "password": "password123",
        "email": "expired@example.com"
    })

    # Get user
    user = auth.get_user("expireduser", "password123")

    # Create token that expires in -1 minutes (already expired)
    token, _ = auth.create_access_token(user, -1)

    # Verify token is invalid
    with pytest.raises(InvalidInputError):
        auth.get_user_from_token(token)
    
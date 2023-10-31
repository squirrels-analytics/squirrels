from typing import Optional
import pytest

from squirrels._auth import Authenticator, UserBase, UserPwd
from squirrels import _constants as c


class AuthHelper:
    class User(UserBase):
        def __init__(self, username="", is_admin=False, email="", **kwargs):
            super().__init__(username, is_internal=is_admin, **kwargs)
            self.email = email
        
        def __eq__(self, other) -> bool:
            return type(other) is self.__class__ and self.__dict__ == other.__dict__
    
    def get_user_and_hashed_pwd(self, username: str) -> Optional[UserPwd]:
        mock_db = {
            "johndoe": {
                "username": "johndoe",
                "email": "john.doe@email.com",
                "is_admin": True,
                "password": str(hash("secret"))
            },
            "mattdoe": {
                "username": "mattdoe",
                "email": "matt.doe@email.com",
                "is_admin": False,
                "password": str(hash("secret"))
            }
        }
        if username in mock_db:
            record = mock_db[username]
            user = self.User(**record)
            return UserPwd(user, record["password"])
    
    def verify_pwd(self, login_pwd: str, hashed_pwd: str) -> bool:
        return str(hash(login_pwd)) == hashed_pwd


@pytest.fixture
def auth() -> Authenticator:
    return Authenticator(30, AuthHelper())


@pytest.fixture
def john_doe_user() -> UserBase:
    return AuthHelper.User("johndoe", is_admin=True, email="john.doe@email.com")


@pytest.fixture
def matt_doe_user() -> UserBase:
    return AuthHelper.User("mattdoe", is_admin=False, email="matt.doe@email.com")


@pytest.mark.parametrize('username,password,expected', [
    ("johndoe", "secret", "john_doe_user"),
    ("johndoe", "wrong", None),
    ("wrong", "secret", None),
])
def test_authenticate_user(username: str, password: str, expected: Optional[str], auth: Authenticator, request: pytest.FixtureRequest):
    expected_user = None if expected is None else request.getfixturevalue(expected)
    retrieved_user = auth.authenticate_user(username, password)
    assert retrieved_user == expected_user


@pytest.mark.parametrize('user_fixture', [
    ('john_doe_user'), ('matt_doe_user')
])
def test_get_user_from_token(user_fixture: str, auth: Authenticator, request: pytest.FixtureRequest):
    input_user = request.getfixturevalue(user_fixture)
    token, _ = auth.create_access_token(input_user)
    user = auth.get_user_from_token(token)
    assert user == input_user


@pytest.mark.parametrize('user_fixture,public,protected,private', [
    ('john_doe_user', True, True, True), 
    ('matt_doe_user', True, True, False),
    (None, True, False, False)
])
def test_can_user_access_scope(user_fixture: str, public: bool, protected: bool, private: bool,
                               auth: Authenticator, request: pytest.FixtureRequest):
    input_user = request.getfixturevalue(user_fixture) if user_fixture is not None else None
    assert auth.can_user_access_scope(input_user, c.PUBLIC_SCOPE) == public
    assert auth.can_user_access_scope(input_user, c.PROTECTED_SCOPE) == protected
    assert auth.can_user_access_scope(input_user, c.PRIVATE_SCOPE) == private

from typing import Optional
import pytest

from squirrels._authenticator import Authenticator, UserBase, WrongPassword
from squirrels import _constants as c


class AuthHelper:
    class User(UserBase):
        def __init__(self, username="", is_admin=False, email="", **kwargs):
            super().__init__(username, is_internal=is_admin, **kwargs)
            self.email = email
        
        def __eq__(self, other) -> bool:
            return type(other) is self.__class__ and self.__dict__ == other.__dict__
    
    def get_user_if_valid(self, username: str, password: str) -> Optional[UserBase]:
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
            if str(hash(password)) == record["password"]:
                return self.User(**record)
            else:
                return WrongPassword(username)


@pytest.fixture(scope="module")
def auth() -> Authenticator:
    return Authenticator(30, AuthHelper())


@pytest.fixture(scope="module")
def john_doe_user() -> AuthHelper.User:
    return AuthHelper.User("johndoe", is_admin=True, email="john.doe@email.com")


@pytest.fixture(scope="module")
def matt_doe_user() -> AuthHelper.User:
    return AuthHelper.User("mattdoe", is_admin=False, email="matt.doe@email.com")


@pytest.fixture(scope="module")
def lisa_doe_user() -> AuthHelper.User:
    return AuthHelper.User("lisadoe", is_admin=True, email="")


@pytest.mark.parametrize('username,password,expected', [
    ("johndoe", "secret", "john_doe_user"),
    ("johndoe", "qwerty", None),
    ("johndoe", "wrong", None),
    ("lisadoe", "abcd1234", "lisa_doe_user"),
    ("wrong", "secret", None),
])
def test_authenticate_user(username: str, password: str, expected: Optional[str], auth: Authenticator, request: pytest.FixtureRequest):
    expected_user = None if expected is None else request.getfixturevalue(expected)
    retrieved_user = auth.authenticate_user(username, password)
    assert retrieved_user == expected_user


@pytest.mark.parametrize('user_fixture', [
    ('john_doe_user'), ('matt_doe_user'), ('lisa_doe_user')
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

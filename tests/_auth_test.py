from typing import Optional
import pytest

from squirrels._auth import Authenticator, UserBase, UserPwd


class AuthHelper:
    class User(UserBase):
        def __init__(self, username, email, **kwargs):
            super().__init__(username, **kwargs)
            self.email = email
        
        def __eq__(self, other) -> bool:
            return type(other) is self.__class__ and self.__dict__ == other.__dict__
    
    def get_user_and_hashed_pwd(self, username: str) -> Optional[UserPwd]:
        mock_db = {
            "johndoe": {
                "username": "johndoe",
                "email": "john.doe@email.com",
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
    return AuthHelper.User("johndoe", "john.doe@email.com")


@pytest.mark.parametrize('username,password,expected', [
    ("johndoe", "secret", "john_doe_user"),
    ("johndoe", "wrong", None),
    ("wrong", "secret", None),
])
def test_authenticate_user(username: str, password: str, expected: Optional[str], auth: Authenticator, request: pytest.FixtureRequest):
    expected_user = None if expected is None else request.getfixturevalue(expected)
    assert auth.authenticate_user(username, password) == expected_user


def test_get_user_from_token(auth: Authenticator, john_doe_user: UserBase):
    token = auth.create_access_token(john_doe_user)
    user = auth.get_user_from_token(token)
    assert user == john_doe_user

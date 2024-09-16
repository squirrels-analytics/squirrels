from typing import Optional, Union
import pytest

from squirrels import AuthArgs
from squirrels._authenticator import Authenticator, User as UserBase, WrongPassword
from squirrels._manifest import DatasetScope


class AuthHelper:    
    class User(UserBase):
        def set_attributes(self, **kwargs) -> None:
            self.email = kwargs["email"]
        
        def __eq__(self, other) -> bool:
            return type(other) is self.__class__ and self.__dict__ == other.__dict__
    
    def get_user_if_valid(self, sqrl: AuthArgs) -> Union[User, WrongPassword, None]:
        mock_db = {
            "johndoe": {
                "username": "johndoe",
                "email": "john.doe@email.com",
                "is_admin": True,
                "hashed_password": str(hash("secret"))
            },
            "mattdoe": {
                "username": "mattdoe",
                "email": "matt.doe@email.com",
                "is_admin": False,
                "hashed_password": str(hash("secret"))
            }
        }

        username, password = sqrl.username, sqrl.password
        if username in mock_db:
            user_obj = mock_db[username]
            if str(hash(password)) == user_obj["hashed_password"]:
                is_admin = user_obj["is_admin"]
                return self.User.Create(username, is_internal=is_admin, email=user_obj["email"])
            else:
                return WrongPassword()


@pytest.fixture(scope="module")
def auth(simple_env_config, simple_conn_args, simple_conn_set) -> Authenticator:
    return Authenticator(".", simple_env_config, simple_conn_args, simple_conn_set, 30, auth_helper=AuthHelper())


@pytest.fixture(scope="module")
def john_doe_user() -> AuthHelper.User:
    return AuthHelper.User.Create("johndoe", is_internal=True, email="john.doe@email.com")


@pytest.fixture(scope="module")
def matt_doe_user() -> AuthHelper.User:
    return AuthHelper.User.Create("mattdoe", is_internal=False, email="matt.doe@email.com")


@pytest.fixture(scope="module")
def lisa_doe_user() -> AuthHelper.User:
    return AuthHelper.User.Create("lisadoe", is_internal=True, email="lisadoe@org2.com")


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
    assert auth.can_user_access_scope(input_user, DatasetScope.PUBLIC) == public
    assert auth.can_user_access_scope(input_user, DatasetScope.PROTECTED) == protected
    assert auth.can_user_access_scope(input_user, DatasetScope.PRIVATE) == private

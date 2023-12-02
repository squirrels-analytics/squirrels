from __future__ import annotations
from typing import Union
from squirrels import UserBase, WrongPassword


class User(UserBase):
    def with_attributes(self, organization: str, **kwargs) -> User:
        """
        Use this method to add custom attributes in the User model that don't exist in UserBase (username, is_internal, etc.)
        """
        self.organization = organization
        return self


def get_user_if_valid(username: str, password: str, **kwargs) -> Union[User, WrongPassword, None]:
    """
    This function allows the squirrels framework to know how to authenticate input username and password.

    Return:
        - User instance: if username and password are correct
        - WrongPassword(username): if username exists but password is incorrect
        - None: if the username doesn't exist (and continue username search among "fake users" in environcfg.yaml)
    """
    mock_users_db = {
        "johndoe": {
            "username": "johndoe",
            "is_admin": True,
            "organization": "org1",
            "hashed_password": str(hash("I<3Squirrels"))
        },
        "mattdoe": {
            "username": "mattdoe",
            "is_admin": False,
            "organization": "org2",
            "hashed_password": str(hash("abcd5678"))
        }
    }

    if username in mock_users_db:
        user_dict = mock_users_db[username]
        hashed_pwd = user_dict["hashed_password"]
        if str(hash(password)) == hashed_pwd:
            is_admin = user_dict["is_admin"]
            return User(username, is_internal=is_admin).with_attributes(**user_dict)
        else:
            return WrongPassword(username)
    return None

from __future__ import annotations
from typing import Union, Any
from squirrels import User as UserBase, WrongPassword


class User(UserBase):
    def set_attributes(self, user_dict: dict[str, Any]) -> None:
        """
        Use this method to add custom attributes in the User model that don't exist in UserBase (username, is_internal, etc.)
        """
        self.organization = user_dict["organization"]


def get_user_if_valid(username: str, password: str) -> Union[User, WrongPassword, None]:
    """
    This function allows the squirrels framework to know how to authenticate input username and password.

    Return:
        - User instance - if username and password are correct
        - WrongPassword(username) - if username exists but password is incorrect
        - None - if the username doesn't exist (and continue username search among "fake users" in environcfg.yml)
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
            user = User(username, is_internal=is_admin)
            return user.with_attributes(user_dict)
        else:
            return WrongPassword(username)
    return None

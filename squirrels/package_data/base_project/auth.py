from typing import Union
from squirrels import UserBase, WrongPassword


class User(UserBase):
    def __init__(self, username='', is_admin=False, organization='', **kwargs):
        super().__init__(username, is_internal=is_admin)
        self.organization = organization


def get_user_if_valid(username: str, password: str, **kwargs) -> Union[User, WrongPassword, None]:
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
            user = User(**user_dict)
            return user
        else:
            return WrongPassword(username)
    return None

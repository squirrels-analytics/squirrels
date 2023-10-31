mock_users_db = {
    "johndoe": {
        "username": "johndoe",
        "is_admin": True,
        "full_name": "John Doe",
        "email": "johndoe@squirrels.com",
        "organization": "squirrels",
        "hashed_password": str(hash("I<3Squirrels"))
    },
    "mattperry": {
        "username": "mattperry",
        "is_admin": False,
        "full_name": "Matthew Perry",
        "email": "mattperry@friends.com",
        "organization": "friends",
        "hashed_password": str(hash("ChandlerRocks0"))
    }
}

from typing import Optional

from squirrels import UserBase, UserPwd


class User(UserBase):
    def __init__(self, username='', is_admin=False, organization='', **kwargs):
        super().__init__(username, is_internal=is_admin)
        self.organization = organization


def get_user_and_hashed_pwd(username: str) -> Optional[UserPwd]:
    if username in mock_users_db:
        user_dict = mock_users_db[username]
        user = User(**user_dict)
        hashed_pwd = user_dict["hashed_password"]
        return UserPwd(user, hashed_pwd)


def verify_pwd(login_pwd: str, hashed_pwd: str) -> bool:
    return str(hash(login_pwd)) == hashed_pwd
    
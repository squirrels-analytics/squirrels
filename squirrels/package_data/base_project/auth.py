mock_users_db = {
    "admin": {
        "username": "admin",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": str(hash("I<3Squirrels"))
    }
}

from typing import Optional

from squirrels import UserBase, UserPwd


class User(UserBase):
    def __init__(self, username, full_name, **kwargs):
        super().__init__(username)
        self.full_name = full_name


def get_user_and_hashed_pwd(username: str) -> Optional[UserPwd]:
    if username in mock_users_db:
        user_dict = mock_users_db[username]
        user = User(**user_dict)
        hashed_pwd = user_dict["hashed_password"]
        return UserPwd(user, hashed_pwd)


def verify_pwd(login_pwd: str, hashed_pwd: str) -> bool:
    return str(hash(login_pwd)) == hashed_pwd
    
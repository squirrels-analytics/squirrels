from typing import Union
from squirrels import User as UserBase, AuthArgs, WrongPassword


class User(UserBase):
    def set_attributes(self, **kwargs) -> None:
        """
        Use this method to add custom attributes in the User model that don't exist in UserBase 
        (i.e., anything that's not 'username' or 'is_internal')
        """
        self.role = kwargs["role"]


def get_user_if_valid(sqrl: AuthArgs) -> Union[User, WrongPassword, None]:
    """
    This function allows the squirrels framework to know how to authenticate input username and password.

    Return:
        - User instance - if username and password are correct
        - WrongPassword() - if username exists but password is incorrect
        - None - if the username doesn't exist (and search for username will continue for "fake users" configured in env.yml)
    """
    mock_users_db = {
        "johndoe": {
            "username": "johndoe",
            "is_admin": True,
            "role": "manager",
            "hashed_password": str(hash("I<3Squirrels"))
        },
        "mattdoe": {
            "username": "mattdoe",
            "is_admin": False,
            "role": "customer",
            "hashed_password": str(hash("abcd5678"))
        }
    }

    user_obj = mock_users_db.get(sqrl.username)
    if user_obj is None:
        return None
    
    if str(hash(sqrl.password)) == user_obj["hashed_password"]:
        return User.Create(sqrl.username, is_internal=user_obj["is_admin"], role=user_obj["role"])
    else:
        return WrongPassword()

from typing import Union, Any
from squirrels import User as UserBase, AuthArgs, WrongPassword


class User(UserBase):
    def set_attributes(self, user_dict: dict[str, Any]) -> None:
        """
        Use this method to add custom attributes in the User model that don't exist in UserBase (username, is_internal, etc.)
        """
        self.organization = user_dict["organization"]


def get_user_if_valid(sqrl: AuthArgs) -> Union[User, WrongPassword, None]:
    """
    This function allows the squirrels framework to know how to authenticate input username and password.

    Return:
        - User instance - if username and password are correct
        - WrongPassword() - if username exists but password is incorrect
        - None - if the username doesn't exist (and search for username will continue for "fake users" configured in environcfg.yml)
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

    user_dict = mock_users_db.get(sqrl.username)
    if user_dict is None:
        return None
    
    if str(hash(sqrl.password)) == user_dict["hashed_password"]:
        return User.Create(sqrl.username, user_dict, is_internal=user_dict["is_admin"])
    else:
        return WrongPassword()

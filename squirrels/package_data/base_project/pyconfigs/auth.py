from squirrels import User, AuthLoginArgs, AuthTokenArgs

# Remove these variables once you're using a real database
mock_users = [
    {
        "username": "johndoe",
        "is_admin": True,
        "role": "manager",
        "hashed_password": str(hash("I<3Squirrels"))
    },
    {
        "username": "mattdoe",
        "is_admin": False,
        "role": "customer",
        "hashed_password": str(hash("abcd5678"))
    }
]
mock_users_by_username = {user["username"]: user for user in mock_users}


def get_user_from_login(sqrl: AuthLoginArgs) -> User | None:
    """
    This function allows the squirrels framework to know how to authenticate input username and password.

    Return:
        - User instance - if username and password are correct
        - None - if the username doesn't exist (and search for username will continue for "fake users" configured in env.yml)
    """
    user_obj = mock_users_by_username.get(sqrl.username)
    if user_obj is not None and str(hash(sqrl.password)) == user_obj["hashed_password"]:
        return User.Create(sqrl.username, is_internal=user_obj["is_admin"], role=user_obj["role"])

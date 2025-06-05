from typing import Literal
from squirrels import auth, arguments as args


class User(auth.BaseUser):
    """
    Extend the BaseUser class with custom attributes. The attributes defined here will be added as columns to the users table. 
    - Only the following types are supported: [str, int, float, bool, typing.Literal]
    - For str, int, and float types, add "| None" after the type to make it nullable. 
    - Always set a default value for the column (use None if default is null).
    
    Example:
        organization: str | None = None
    """
    role: Literal["manager", "employee"] = "employee"

    @classmethod
    def dropped_columns(cls) -> list[str]:
        """
        The fields defined above cannot be modified once added to the database. 
        However, you can choose to drop columns by adding them to this list.
        """
        return []


@auth.provider(name="google", label="Google", icon="https://www.google.com/favicon.ico")
def google_auth_provider(sqrl: args.AuthProviderArgs) -> auth.ProviderConfigs:
    """
    Provider configs for authenticating a user using Google credentials.

    See the following page for setting up the CLIENT_ID and CLIENT_SECRET for Google specifically: 
    https://support.google.com/googleapi/answer/6158849?hl=en
    """
    def get_sqrl_user(claims: dict) -> User:
        return User(
            username=claims["email"],
            is_admin=False,
            role="employee"
        )

    # TODO: Add GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET to the .env file
    provider_configs = auth.ProviderConfigs(
        client_id="", # sqrl.env_vars["GOOGLE_CLIENT_ID"],
        client_secret="", # sqrl.env_vars["GOOGLE_CLIENT_SECRET"],
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
        get_user=get_sqrl_user
    )

    return provider_configs

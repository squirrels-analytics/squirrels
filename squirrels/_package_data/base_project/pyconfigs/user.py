from typing import Literal
from squirrels import auth, arguments as args


class CustomUserFields(auth.CustomUserFields):
    """
    Extend the CustomUserFields class to add custom user attributes. 
    - Only the following types are supported: [str, int, float, bool, typing.Literal]
    - Add "| None" after the type to make it nullable. 
    - Always set a default value for the field (use None if default is null).
    
    Example:
        organization: str | None = None
    """
    role: Literal["manager", "employee"] = "employee"


# @auth.provider(name="google", label="Google", icon="https://www.google.com/favicon.ico")
def google_auth_provider(sqrl: args.AuthProviderArgs) -> auth.ProviderConfigs:
    """
    Provider configs for authenticating a user using Google credentials.

    See the following page for setting up the CLIENT_ID and CLIENT_SECRET for Google specifically: 
    https://support.google.com/googleapi/answer/6158849?hl=en
    """
    def get_sqrl_user(claims: dict) -> auth.RegisteredUser:
        custom_fields = CustomUserFields(role="employee")
        return auth.RegisteredUser(
            username=claims["email"],
            access_level="member",
            custom_fields=custom_fields
        )

    # TODO: Add GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET to the .env file
    # Then, uncomment the @auth.provider decorator above and set the client_id and client_secret below
    provider_configs = auth.ProviderConfigs(
        client_id="", # sqrl.env_vars["GOOGLE_CLIENT_ID"],
        client_secret="", # sqrl.env_vars["GOOGLE_CLIENT_SECRET"],
        server_url="https://accounts.google.com",
        client_kwargs={"scope": "openid email profile"},
        get_user=get_sqrl_user
    )

    return provider_configs

from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class UserBase:
    """
    Base class for extending the custom User model class

    Attributes:
        username: The identifier for the user
        is_internal: Setting this to True lets the user access "private" datasets
    """
    username: str
    is_internal: bool

    def __init__(self, username: str, *, is_internal: bool = False, **kwargs):
        """
        Constructor for the UserBase class

        Parameters:
            ...see Attributes of UserBase
        """
        self.username = username
        self.is_internal = is_internal
    
    def __hash__(self) -> int:
        return hash(self.username)
    
    @classmethod
    def _FromDict(cls, user_dict: Dict[str, Any]):
        user = cls()
        for key, val in user_dict.items():
            setattr(user, key, val)
        return user


@dataclass
class WrongPassword:
    """
    Return this object if the username was found but the password was incorrect

    This ensures that if the username exists as a real user, we won't continue to use the environcfg.yaml file to authenticate

    Attributes:
        username: The identifier for the user
    """
    username: str

import typing as _t, dataclasses as _dc


@_dc.dataclass
class User:
    """
    Base class for extending the custom User model class

    Attributes:
        username: The identifier for the user
        is_internal: Setting this to True lets the user access "private" datasets
    """
    username: str
    is_internal: bool
    
    def __hash__(self) -> int:
        return hash(self.username)
    
    def __str__(self) -> str:
        return self.username
    
    def set_attributes(self, **kwargs) -> None:
        """
        Can be overwritten in "auth.py" to introduce custom attributes. Does nothing by default
        """
        pass
    
    @classmethod
    def Create(cls, username: str, *, is_internal: bool = False, **kwargs):
        """
        Creates an instance of the User class and calls the `set_attributes` method on the new instance.

        We may overwrite the `set_attributes` method in `auth.py`. We do not overwrite the constructor to guarantee that `username` and `is_internal` are always set.

        Arguments:
            username: The identifier for the user
            is_internal: Setting this to True lets the user access "private" datasets. Default is False
        """
        user = cls(username, is_internal)
        user.set_attributes(**kwargs)
        return user
    
    @classmethod
    def _FromDict(cls, user_obj_as_dict: dict[str, _t.Any]):
        username, is_internal = user_obj_as_dict["username"], user_obj_as_dict["is_internal"]
        user = cls(username=username, is_internal=is_internal)
        for key, val in user_obj_as_dict.items():
            setattr(user, key, val)
        return user


@_dc.dataclass
class WrongPassword:
    """
    Return this object if the username was found but the password was incorrect

    This ensures that if the username exists as a real user, we won't continue to use the env.yml file to authenticate
    """

from typing import Any
from squirrels import User as UserBase


class User(UserBase):
    def set_attributes(self, user_dict: dict[str, Any]) -> None:
        self.organization = user_dict["organization"]

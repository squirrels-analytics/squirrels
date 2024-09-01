from squirrels import User as UserBase


class User(UserBase):
    def set_attributes(self, **kwargs) -> None:
        self.organization = kwargs["organization"]

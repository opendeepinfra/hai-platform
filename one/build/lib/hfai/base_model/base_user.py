
import secrets
import string
import random
import uuid


class BaseUser:
    def __init__(self, user_name, user_id, token, role, **kwargs):
        self.user_name = user_name
        self.user_id = user_id
        if token is None:
            token = ''.join(random.choices(string.ascii_letters, k=4)) + secrets.token_urlsafe(12)
        self.token = token
        self.role = role
        self.shared_group = kwargs.get('shared_group')
        self.nick_name = kwargs.get('nick_name', self.user_name)

    def __repr__(self):
        self_dict = self.__dict__
        return '\n'.join([f'{k}: {self_dict[k]}' for k in self_dict])

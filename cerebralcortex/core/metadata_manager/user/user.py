# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from datetime import datetime
from uuid import UUID


class User:
    def __init__(self,
                 user_id: UUID,
                 username: str,
                 password: str,
                 token: str = None,
                 token_issued_at: datetime = None,
                 token_expiry: datetime = None,
                 user_role: datetime = None,
                 user_metadata: dict = None,
                 active: bool = 1):
        """
        Contains user metadata information

        Args:
            user_id (str):
            username (str):
            password (str):
            token (str):
            token_issued_at (datetime):
            token_expiry (datetime):
            user_role (str):
            user_metadata (dict):
            active int:
        """
        self._identifier = user_id
        self._username = username
        self._password = password
        self._token = token
        self._token_issued_at = token_issued_at
        self._token_expiry = token_expiry
        self._user_role = user_role
        self._user_metadata = user_metadata
        self._active = active

    @property
    def user_id(self):
        """

        Returns:
            (str): user id

        """
        return self._identifier

    @user_id.setter
    def user_id(self, val:str):
        """

        Args:
            val (str): user id
        """
        self._identifier = val

    @property
    def username(self):
        """

        Returns:
            (str): user name

        """
        return self._username

    @username.setter
    def username(self, val):
        """

        Args:
            val (str): user name
        """
        self._username = val

    @property
    def password(self):
        """

        Returns:
            (str): encrypted password

        """
        return self._password

    @password.setter
    def password(self, val):
        """

        Args:
            val (str): encrypted password
        """
        self._password = val

    @property
    def token(self):
        """

        Returns:
            (str): auth token

        """
        return self._token

    @token.setter
    def token(self, val):
        """

        Args:
            val (str): auth token
        """
        self._token = val

    @property
    def token_issued_at(self):
        """

        Returns:
            (datetime): date and time when token was issues

        """
        return self._token_issued_at

    @token_issued_at.setter
    def token_issued_at(self, val):
        """

        Args:
            val (datetime): date and time when token was issues
        """
        self.token_issued_at = val

    @property
    def token_expiry(self):
        """

        Returns:
            (datetime): date and time when token will expire

        """
        return self._token_expiry

    @token_expiry.setter
    def token_expiry(self, val):
        """

        Args:
            val: (datetime): date and time when token will expire
        """
        self._token_expiry = val

    @property
    def user_role(self):
        """

        Returns (str): role

        """
        return self._user_role

    @user_role.setter
    def user_role(self, val):
        """

        Args:
            val (str): role
        """
        self._user_role = val

    @property
    def user_metadata(self):
        """

        Returns (dict): metadata of a user

        """
        return self._user_metadata

    @user_metadata.setter
    def user_metadata(self, val):
        """

        Args:
            val (dict): metadata of a user
        """
        self._user_metadata = val

    @property
    def isactive(self):
        """

        Returns (int): user status

        """
        return self._active

    @isactive.setter
    def isactive(self, val):
        """

        Args:
            val (int): user status
        """
        self._active = val

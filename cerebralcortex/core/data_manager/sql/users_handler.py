# Copyright (c) 2018, MD2K Center of Excellence
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

import hashlib
import random
import string
import uuid
from datetime import datetime

from pytz import timezone


class UserHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_user_metadata(self, user_id: uuid = None, username: str = None) -> list(dict):
        """
        Get user metadata by user_id or by username

        Args:
            user_id (str): id (uuid) of a user
            user_name (str): username of a user
        Returns:
            list(dict): List of dictionaries of user metadata
        Todo:
            Return list of User class object
        Raises:
            ValueError: User ID/name cannot be empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_metadata(username="nasir_ali")
            >>> [{"study_name":"mperf"........}]
        """
        result = []
        if not user_id and not username:
            raise ValueError("User ID/name cannot be empty.")

        if user_id and not username:
            qry = "select user_metadata from user where identifier=%(identifier)s"
            vals = {"identifier": str(user_id)}
        elif not user_id and username:
            qry = "select user_metadata from user where username=%(username)s"
            vals = {"username": str(username)}
        else:
            qry = "select user_metadata from user where identifier=%s and username=%s"
            vals = str(user_id), str(username)

        rows = self.execute(qry, vals)
        if len(rows) > 0:
            for row in rows:
                result.append(row)
            return result
        else:
            return []

    def get_user_uuid(self, username: str) -> str:

        """
        Find user UUID of a user name
        :param username:
        :return: string format of a user UUID
        :rtype: str
        """

        qry = "SELECT identifier from " + self.userTable + " where username = %(username)s"
        vals = {'username': str(username)}
        rows = self.execute(qry, vals)

        if rows:
            user_uuid = rows[0]["identifier"]
            return user_uuid
        else:
            return ""

    def login_user(self, username: str, password: str) -> bool:
        """
        Authenticate a user based on username and password
        :param username:
        :param password:
        :return:

        Args:
            username (str):  username of a user
            password (str): Encrypted password of a user
        Raises:
            ValueError: User name and password cannot be empty/None.
        Returns:
            bool: returns True on successful login or False otherwise.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.connect("nasir_ali", "2ksdfhoi2r2ljndf823hlkf8234hohwef0234hlkjwer98u234")
            >>> True
        """
        if not username or not password:
            raise ValueError("User name and password cannot be empty/None.")

        qry = "select * from user where username=%s and password=%s"
        vals = username, password

        rows = self.execute(qry, vals)
        if len(rows) == 0:
            return False
        else:
            return True

    def is_auth_token_valid(self, username: str, auth_token: str, auth_token_expiry_time: datetime) -> bool:
        """
        Validate whether a token is valid or expired based on the token expiry datetime stored in SQL

        Args:
            username (str): username of a user
            auth_token (str): token generated by API-Server
            auth_token_expiry_time (datetime): current datetime object
        Raises:
            ValueError: Auth token and auth-token expiry time cannot be null/empty.
        Returns:
            bool: returns True if token is valid or False otherwise.
        """
        if not auth_token or not auth_token_expiry_time:
            raise ValueError("Auth token and auth-token expiry time cannot be null/empty.")

        qry = "select * from user where token=%s and username=%s"
        vals = auth_token, username

        rows = self.execute(qry, vals)

        if len(rows) == 0:
            return False
        else:
            token_expiry_time = rows[0]["token_expiry"]
            localtz = timezone(self.time_zone)
            token_expiry_time = localtz.localize(token_expiry_time)

            if token_expiry_time < auth_token_expiry_time:
                return False
            else:
                return True

    ###################################################################
    ################## STORE DATA METHODS #############################
    ###################################################################

    def update_auth_token(self, username: str, auth_token: str, auth_token_issued_time: datetime,
                          auth_token_expiry_time: datetime) -> bool:
        """
        Update an auth token in SQL database to keep user stay logged in. Auth token valid duration can be changed in configuration files.

        Args:
            username (str): username of a user
            auth_token (str): issued new auth token
            auth_token_issued_time (datetime): datetime when the old auth token was issue
            auth_token_expiry_time (datetime): datetime when the token will get expired
        Raises:
            ValueError: Auth token and auth-token issue/expiry time cannot be None/empty.
        Returns:
            bool: Returns True if the new auth token is set or False otherwise.

        """
        if not auth_token and not auth_token_expiry_time and not auth_token_issued_time:
            raise ValueError("Auth token and auth-token issue/expiry time cannot be None/empty.")

        qry = "UPDATE " + self.userTable + " set token=%s, token_issued=%s, token_expiry=%s where username=%s"
        vals = auth_token, auth_token_issued_time, auth_token_expiry_time, username

        user_uuid = self.get_user_uuid(username)
        try:
            self.execute(qry, vals, commit=True)
            return True
        except:
            return False

    def gen_random_pass(self, string_type: str, size: int = 8) -> str:
        """
        Generate a random password
        Args:
            string_type: Accepted parameters are "varchar" and "char". (Default="varchar")
            size: password length (default=8)

        Returns:
            str: random password

        """
        if (string_type == "varchar"):
            chars = string.ascii_lowercase + string.digits
        elif (string_type == "char"):
            chars = string.ascii_lowercase
        else:
            chars = string.digits

        return ''.join(random.choice(chars) for _ in range(size))

    def encrypt_user_password(self, user_password: str) -> str:
        """
        Encrypt password
        Args:
            user_password (str): unencrypted password
        Raises:
             ValueError: password cannot be None or empty.
        Returns:
            str: encrypted password
        """
        if user_password is None or user_password=="":
            raise ValueError("password cannot be None or empty.")
        hash_pwd = hashlib.sha256(user_password.encode('utf-8'))
        return hash_pwd.hexdigest()

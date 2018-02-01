# Copyright (c) 2017, MD2K Center of Excellence
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
from typing import List

from pytz import timezone


class UserHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def get_user_metadata(self, user_id:uuid, username: str = None) -> List:
        """

        :param user_id:
        :param username:
        :return:
        """
        if not user_id and not username:
            return None

        if user_id and not username:
            qry = "select user_metadata from user where identifier=%(identifier)s"
            vals = {"identifier": str(user_id)}
        elif not user_id and username:
            qry = "select user_metadata from user where username=%(username)s"
            vals = {"username":str(username)}
        else:
            qry = "select user_metadata from user where identifier=%s and username=%s"
            vals = str(user_id), str(username)

        rows = self.execute(qry, vals)
        #rows = self.cursor.fetchall()
        if len(rows) == 0:
            return []
        else:
            return rows[0]["user_metadata"]
    def get_user_uuid(self, username: str) -> str:

        """

        :param username:
        :return:
        """

        qry = "SELECT identifier from " + self.userTable + " where username = %(username)s"
        vals = {'username': str(username)}
        rows = self.cexecute(qry, vals)
        #rows = self.cursor.fetchall()

        if rows:
            user_uuid = rows[0]["identifier"]
            return user_uuid
        else:
            return False

    def login_user(self, username: str, password: str) -> bool:
        """

        :param username:
        :param password:
        :return:
        """
        if not username or not password:
            raise ValueError("User name and password cannot be empty/null.")

        qry = "select * from user where username=%s and password=%s"
        vals = username, password

        rows = self.execute(qry, vals)
        #rows = self.cursor.fetchall()
        if len(rows) == 0:
            return False
        else:
            return True

    def is_auth_token_valid(self, token_owner: str, auth_token: str, auth_token_expiry_time: datetime) -> bool:
        """

        :param token_owner:
        :param auth_token:
        :param auth_token_expiry_time:
        :return:
        """
        if not auth_token or not auth_token_expiry_time:
            raise ValueError("Auth token and auth-token expiry time cannot be null/empty.")

        qry = "select * from user where token=%s and username=%s"
        vals = auth_token, token_owner

        rows = self.execute(qry, vals)
        #rows = self.cursor.fetchall()

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
                          auth_token_expiry_time: datetime) -> str:

        """

        :param username:
        :param auth_token:
        :param auth_token_issued_time:
        :param auth_token_expiry_time:
        :return uuid of the current user
        """
        if not auth_token and not auth_token_expiry_time and not auth_token_issued_time:
            raise ValueError("Auth token and auth-token issue/expiry time cannot be null/empty.")

        qry = "UPDATE " + self.userTable + " set token=%s, token_issued=%s, token_expiry=%s where username=%s"
        vals = auth_token, auth_token_issued_time, auth_token_expiry_time, username

        user_uuid = self.get_user_uuid(username)

        self.execute(qry, vals, commit=True)
        #self.dbConnection.commit()

        return user_uuid

    def gen_random_pass(self, string_type: str, size: int = 8) -> str:
        """
        :param string_type:
        :param size:
        :return:
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
        :param user_password:
        :return:
        """
        hash_pwd = hashlib.sha256(user_password.encode('utf-8'))
        return hash_pwd.hexdigest()

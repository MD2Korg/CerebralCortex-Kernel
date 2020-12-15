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

import hashlib
import random
import re
import string
import uuid
from datetime import datetime
from datetime import timedelta
from typing import List

import jwt
from pytz import timezone

from cerebralcortex.core.data_manager.sql.orm_models import User


class UserHandler():

    ###################################################################
    ################## GET DATA METHODS ###############################
    ###################################################################

    def create_user(self, username:str, user_password:str, user_role:str, user_metadata:dict, user_settings:dict, encrypt_password:bool=False)->bool:
        """
        Create a user in SQL storage if it doesn't exist
        Args:
            username (str): Only alphanumeric usernames are allowed with the max length of 25 chars.
            user_password (str): no size limit on password
            user_role (str): role of a user
            user_metadata (dict): metadata of a user
            user_settings (dict): user settings, mCerebrum configurations of a user
            encrypt_password (bool): encrypt password if set to True
        Returns:
            bool: True if user is successfully registered or throws any error in case of failure
        Raises:
            ValueError: if selected username is not available
            Exception: if sql query fails
        """
        self.username_checks(username)
        if self.is_user(user_name=username):
            raise ValueError("username is already registered. Please select another user name")

        user_uuid = str(username)+str(user_role)+str(user_metadata)
        user_uuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, user_uuid))
        if encrypt_password:
            user_password = self.encrypt_user_password(user_password)

        user = User(user_id=user_uuid, username=username, password=user_password, study_name=self.study_name,
                    token="", token_issued=datetime.now(), token_expiry=datetime.now(), user_role=user_role,
                    user_settings=user_settings, user_metadata=user_metadata)

        try:
            self.session.add(user)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise Exception(e)

    def get_user_metadata(self, user_id: uuid = None, username: str = None) -> dict:
        """
        Get user metadata by user_id or by username

        Args:
            user_id (str): id (uuid) of a user
            user_name (str): username of a user
        Returns:
            dict: user metadata
        Todo:
            Return list of User class object
        Raises:
            ValueError: User ID/name cannot be empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_metadata(username="nasir_ali")
            >>> {"study_name":"mperf"........}
        """

        if not user_id and not username:
            raise ValueError("User ID/name cannot be empty.")

        if user_id and not username:
            user = self.session.query(User).filter((User.user_id == user_id) & (User.study_name==self.study_name)).first()
        elif not user_id and username:
            user = self.session.query(User).filter((User.username == username) & (User.study_name == self.study_name)).first()
        else:
            user = self.session.query(User).filter((User.user_id == user_id) & (User.username == username) & (User.study_name == self.study_name)).first()

        self.close()

        if user:
            return user.user_metadata
        else:
            return {}

    def get_user_settings(self, username: str=None, auth_token: str = None) -> dict:
        """
        Get user settings by auth-token or by username. These are user's mCerebrum settings

        Args:
            username (str): username of a user
            auth_token (str): auth-token
        Returns:
            list[dict]: List of dictionaries of user metadata
        Todo:
            Return list of User class object
        Raises:
            ValueError: User ID/name cannot be empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_settings(username="nasir_ali")
            >>> [{"mcerebrum":"some-conf"........}]
        """

        if not username and not auth_token:
            raise ValueError("User ID or auth token cannot be empty.")

        if username and not auth_token:
            user = self.session.query(User.user_settings, User.username, User.user_id).filter((User.username == username) & (User.study_name == self.study_name)).first()
        elif not username and auth_token:
            user = self.session.query(User.user_settings, User.username, User.user_id).filter((User.token == auth_token) & (User.study_name == self.study_name)).first()
        else:
            user = self.session.query(User.user_settings, User.username, User.user_id).filter((User.username == username) & (User.token == auth_token) & (User.study_name == self.study_name)).first()

        self.close()

        if user:
            user_info = user.user_settings
            user_info["username"] = user.username
            user_info["user_id"] = user.user_id
            return user_info
        else:
            return {}

    def login_user(self, username: str, password: str, encrypt_password:bool=False) -> dict:
        """
        Authenticate a user based on username and password and return an auth token

        Args:
            username (str):  username of a user
            password (str): password of a user
            encrypt_password (str): is password encrypted or not. mCerebrum sends encrypted passwords
        Raises:
            ValueError: User name and password cannot be empty/None.
        Returns:
            dict: return eturn {"status":bool, "auth_token": str, "msg": str}
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.connect("nasir_ali", "2ksdfhoi2r2ljndf823hlkf8234hohwef0234hlkjwer98u234", True)
            >>> True
        """
        if not username or not password:
            raise ValueError("User name and password cannot be empty/None.")

        if encrypt_password:
            password = self.encrypt_user_password(password)

        user = self.session.query(User).filter(
            (User.username == username) & (User.password==password) & (User.study_name == self.study_name)).first()

        token_issue_time = datetime.now()
        expires = timedelta(seconds=int(self.config["cc"]['auth_token_expire_time']))
        token_expiry = token_issue_time + expires

        token = jwt.encode({'username': username, "token_expire_at":str(token_expiry), "token_issued_at":str(token_issue_time)}, self.config["cc"]["auth_encryption_key"], algorithm='HS256')
        token = token.decode("utf-8")

        self.close()

        if not user:
            return {"status":False, "auth_token": "", "msg":" Incorrect username and/or password."}
        elif not self.update_auth_token(username, token, token_issue_time, token_expiry):
            return {"status":False, "auth_token": "", "msg": "cannot update auth token."}
        else:
            return {"status":True, "auth_token": token, "msg": "login successful."}

    def is_auth_token_valid(self, username: str, auth_token: str, checktime:bool=False) -> bool:
        """
        Validate whether a token is valid or expired based on the token expiry datetime stored in SQL

        Args:
            username (str): username of a user
            auth_token (str): token generated by API-Server
            checktime (bool): setting this to False will only check if the token is available in system. Setting this to true will check if the token is expired based on the token expiry date.
        Raises:
            ValueError: Auth token and auth-token expiry time cannot be null/empty.
        Returns:
            bool: returns True if token is valid or False otherwise.
        """
        if not auth_token:
            raise ValueError("Auth token cannot be null/empty.")

        user = self.session.query(User).filter(
            (User.username == username) & (User.token == auth_token) & (User.study_name == self.study_name)).first()

        self.close()

        if not user:
            return False
        elif not checktime:
            return True
        else:
            token_expiry_time = user.token_expiry
            localtz = timezone(self.time_zone)
            token_expiry_time = localtz.localize(token_expiry_time)

            if token_expiry_time < datetime.now():
                return False
            else:
                return True

    def list_users(self) -> List[list]:
        """
        Get a list of all users part of a study.

        Args:
            study_name (str): name of a study. If no study_name is provided then all users' list will be returned
        Raises:
            ValueError: Study name is a requied field.
        Returns:
            list[list]: Returns empty list if there is no user associated to the study_name and/or study_name does not exist.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.list_users("mperf")
            >>> [{"76cc444c-4fb8-776e-2872-9472b4e66b16": "nasir_ali"}] # [{user_id, user_name}]
        """

        rows = self.session.query(User.user_id, User.username).filter(User.study_name == self.study_name).all()

        self.close()

        if rows:
            return rows
        else:
            return []

    def get_username(self, user_id: str) -> str:
        """
        Get the user name linked to a user id.

        Args:
            user_name (str): username of a user
        Returns:
            bool: user_id associated to username
        Raises:
            ValueError: User ID is a required field.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_username("76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> 'nasir_ali'
        """
        if not user_id:
            raise ValueError("User ID is a required field.")

        user = self.session.query(User.username).filter((User.user_id==user_id) & (User.study_name == self.study_name)).first()

        self.close()

        if not user:
            raise Exception(str(user_id)+" does not exist.")
        else:
            return user.username

    def get_user_id(self, user_name: str) -> str:
        """
        Get the user id linked to user_name.

        Args:
            user_name (str): username of a user
        Returns:
            str: user id associated to user_name
        Raises:
            ValueError: User name is a required field.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.get_user_id("nasir_ali")
            >>> '76cc444c-4fb8-776e-2872-9472b4e66b16'
        """
        if not user_name:
            raise ValueError("User name is a required field.")

        user = self.session.query(User.user_id).filter((User.username==user_name) & (User.study_name == self.study_name)).first()

        self.close()

        if not user:
            raise Exception(str(user_name)+ " does not exist.")
        else:
            return user.user_id

    def is_user(self, user_id: uuid = None, user_name: uuid = None) -> bool:
        """
        Checks whether a user exists in the system. One of both parameters could be set to verify whether user exist.

        Args:
            user_id (str): id (uuid) of a user
            user_name (str): username of a user
        Returns:
            bool: True if a user exists in the system or False otherwise.
        Raises:
            ValueError: Both user_id and user_name cannot be None or empty.
        Examples:
            >>> CC = CerebralCortex("/directory/path/of/configs/")
            >>> CC.is_user(user_id="76cc444c-4fb8-776e-2872-9472b4e66b16")
            >>> True
        """

        if user_id and user_name:
            user = self.session.query(User.username).filter(
                (User.user_id == user_id) & (User.username==user_name) & (User.study_name == self.study_name)).first()
        elif user_id and not user_name:
            user = self.session.query(User.username).filter(
                (User.user_id == user_id) & (User.study_name == self.study_name)).first()
        elif not user_id and user_name:
            user = self.session.query(User.username).filter(
                (User.username == user_name) & (User.study_name == self.study_name)).first()
        else:
            raise ValueError("Both user_id and user_name cannot be None or empty.")

        self.close()

        if user:
            return True
        else:
            return False

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

        try:
            self.session.query(User).filter(
                (User.username == username) & (User.study_name == self.study_name)).update({User.token: auth_token,
                                                                                        User.token_issued: auth_token_issued_time,
                                                                                        User.token_expiry: auth_token_expiry_time})
            self.session.commit()
            return True
        except:
            self.session.rollback()
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

    def username_checks(self, username:str):
        """
        No space, special characters, dash etc. are allowed in username.
        Only alphanumeric usernames are allowed with the max length of 25 chars.

        Args:
            username (str):
        Returns:
             bool: True if provided username comply the standard or throw an exception
        Raises:
            Exception: if username doesn't follow standards
        """
        regexp = re.compile('^\w+$')
        if not regexp.search(username) or len(username)>50:
            raise Exception("Only alphanumeric usernames are allowed with the max length of 50 chars.")
        else:
            return True

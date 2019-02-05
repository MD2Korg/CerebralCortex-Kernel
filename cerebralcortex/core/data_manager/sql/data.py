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

from typing import List

import mysql.connector
import mysql.connector.pooling

from cerebralcortex.core.data_manager.sql.cache_handler import CacheHandler
from cerebralcortex.core.data_manager.sql.kafka_offsets_handler import KafkaOffsetsHandler
from cerebralcortex.core.data_manager.sql.stream_handler import StreamHandler
from cerebralcortex.core.data_manager.sql.users_handler import UserHandler
from cerebralcortex.core.log_manager.log_handler import LogTypes


class SqlData(StreamHandler, UserHandler, KafkaOffsetsHandler, CacheHandler):
    def __init__(self, CC):
        """
        Constructor

        Args:
            CC (CerebralCortex): CerebralCortex object reference
        Raises:
            Exception: if none MySQL SQL storage is set in cerebralcortex configurations
        """
        self.config = CC.config

        self.logging = CC.logging
        self.logtypes = LogTypes()
        self.sql_store = self.config["relational_storage"]

        if self.sql_store!="mysql":
            raise Exception(self.sql_store+": SQL storage is not supported. Please install and configure MySQL.")

        self.hostIP = self.config['mysql']['host']
        self.hostPort = self.config['mysql']['port']
        self.database = self.config['mysql']['database']
        self.dbUser = self.config['mysql']['db_user']
        self.dbPassword = self.config['mysql']['db_pass']
        self.datastreamTable = self.config['mysql']['datastream_table']
        self.kafkaOffsetsTable = self.config['mysql']['kafka_offsets_table']
        self.userTable = self.config['mysql']['user_table']
        self.dataReplayTable = self.config['mysql']['data_replay_table']
        self.poolName = self.config['mysql']['connection_pool_name']
        self.poolSize = self.config['mysql']['connection_pool_size']
        self.pool = self.create_pool(pool_name=self.poolName, pool_size=self.poolSize)

    def create_pool(self, pool_name: str = "CC_Pool", pool_size: int = 1):
        """
        Create a connection pool, after created, the request of connecting
        MySQL could get a connection from this pool instead of request to
        create a connection.

        Args:
            pool_name (str): the name of pool, (default="CC_Pool")
            pool_size (int): size of MySQL connections pool (default=1)
        Returns:
            object: MySQL connections pool
        """
        dbconfig = {
            "host": self.hostIP,
            "port": self.hostPort,
            "user": self.dbUser,
            "password": self.dbPassword,
            "database": self.database,
        }

        pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            pool_reset_session=True,
            **dbconfig)
        return pool

    def close(self, conn, cursor):
        """
        close connection of mysql.

        Args:
            conn (object): MySQL connection object
            cursor (object): MySQL cursor object
        Raises:
            Exception: if connection is closed
        """
        try:
            cursor.close()
            conn.close()
        except Exception as exp:
            raise Exception(exp)

    def execute(self, sql, args=None, commit=False)->List[dict]:
        """
        Execute a sql, it could be with args and with out args. The usage is
        similar with execute() function in module pymysql.

        Args:
            sql (str): sql clause
            args (tuple): args need by sql clause
            commit (bool): whether to commit
        Returns:
            list[dict]: returns a list of dicts if commit is set to False
        Raises:
            Exception: if MySQL query fails
        """
        # get connection form connection pool instead of create one.
        conn = self.pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        if args:
            cursor.execute(sql, args)
        else:
            cursor.execute(sql)
        if commit is True:
            conn.commit()
            self.close(conn, cursor)
            return None
        else:
            res = cursor.fetchall()
            self.close(conn, cursor)
            return res

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

import os

from cerebralcortex.CerebralCortex import CerebralCortex

configuration_file = os.path.join(os.path.dirname(__file__), '../cerebralcortex_apiserver.yml')
CC = CerebralCortex(configuration_file, time_zone="America/Los_Angeles", load_spark=False)

# CC.sc.setLogLevel("WARN")

# debug_mode = os.environ.get('FLASK_DEBUG')
# if debug_mode:
#     CC.configuration['apiserver']['debug'] = debug_mode
#
# minio_host = os.environ.get('MINIO_HOST')
# if minio_host:
#     CC.configuration['minio']['host'] = minio_host
# minio_access_key = os.environ.get('MINIO_ACCESS_KEY')
# if minio_access_key:
#     CC.configuration['minio']['access_key'] = minio_access_key
# minio_secret_key = os.environ.get('MINIO_SECRET_KEY')
# if minio_secret_key:
#     CC.configuration['minio']['secret_key'] = minio_secret_key
#
# mysql_host = os.environ.get('MYSQL_HOST')
# if mysql_host:
#     CC.configuration['mysql']['host'] = mysql_host
# mysql_db_user = os.environ.get('MYSQL_DB_USER')
# if mysql_db_user:
#     CC.configuration['mysql']['db_user'] = mysql_db_user
# mysql_db_pass = os.environ.get('MYSQL_DB_PASS')
# if mysql_db_pass:
#     CC.configuration['mysql']['db_pass'] = mysql_db_pass
#
# kafka_host = os.environ.get('KAFKA_HOST')
# if kafka_host:
#     CC.configuration['kafkaserver']['host'] = kafka_host
#
# jwt_secret_key = os.environ.get('JWT_SECRET_KEY')
# if jwt_secret_key:
#     CC.configuration['apiserver']['secret_key'] = jwt_secret_key

# Copyright (c) 2020, MD2K Center of Excellence
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

from sqlalchemy import Column, String, Integer, DateTime, Boolean, JSON, Text, UniqueConstraint

from cerebralcortex.core.data_manager.sql import Base


class Stream(Base):
    __tablename__ = 'stream'
    row_id=Column('row_id',Integer, primary_key=True, autoincrement=True)
    name=Column('name', String(100))
    version=Column('version', Integer)
    study_name = Column('study_name', String(100))
    metadata_hash=Column('metadata_hash', String(100), unique=True, index=True)
    stream_metadata=Column('stream_metadata', JSON)
    creation_date = Column('creation_date', DateTime, default=datetime.now())

    __table_args__ = (UniqueConstraint('name','study_name', 'metadata_hash', name='unique_stream_study_key'),)

    def __init__(self, name, version, study_name, metadata_hash, stream_metadata):
        self.name = name
        self.version = version
        self.study_name = study_name
        self.metadata_hash = metadata_hash
        self.stream_metadata = stream_metadata
        self.creation_date = datetime.now()

    def __repr__(self):
        return "row_id={0}, name={1}, version={2}, metadata_hash={3}, stream_metadata={4}, creation_date={5} \n".format(
        self.row_id, self.name, self.version, self.metadata_hash, self.stream_metadata, self.creation_date
        )


class User(Base):
    __tablename__ = 'user'
    row_id=Column('row_id',Integer, primary_key=True, autoincrement=True)
    user_id = Column('user_id', String(100), unique=True, index=True)
    username=Column('username', String(100), unique=True, index=True)
    password=Column('password', String(100))
    study_name=Column('study_name', String(100))
    token = Column('token', Text)
    token_issued = Column('token_issued', DateTime, default=datetime.now())
    token_expiry = Column('token_expiry', DateTime, default=datetime.now())
    user_role = Column('user_role', String(56))
    user_metadata=Column('user_metadata', JSON)
    user_settings = Column('user_settings', JSON)
    active = Column('active', Boolean)
    has_data = Column('has_data', Boolean)
    creation_date = Column('creation_date', DateTime, default=datetime.now())

    def __init__(self, user_id, username, password, study_name, token, token_issued, token_expiry, user_role="participant", user_metadata={}, user_settings={}, active=1):
        self.user_id = user_id
        self.username = username
        self.password = password
        self.study_name = study_name
        self.token = token
        self.token_issued = token_issued
        self.token_expiry = token_expiry
        self.user_role = user_role
        self.user_metadata = user_metadata
        self.user_settings = user_settings
        self.active = active
        self.creation_date = datetime.now()

    def __repr__(self):
        return "row_id={0}, user_id={1}, username={2}, password={3}, study_name={4}, token={5}, token_issued={6}, " \
               "token_expiry={7}, user_role={8}, user_metadata={9}, user_settings={10}, active={11}, creation_date={12}".format(
            self.row_id, self.user_id, self.username, self.password, self.study_name, self.token, self.token_issued, self.token_expiry,
            self.user_role, self.user_metadata, self.user_settings, self.active, self.creation_date
        )
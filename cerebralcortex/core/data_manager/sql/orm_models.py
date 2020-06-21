
from sqlalchemy import Column, String, Integer, Date, Boolean, Numeric, JSON, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Stream(Base):
    __tablename__ = 'stream'
    row_id=Column('row_id',Integer, primary_key=True, autoincrement=True)
    name=Column('name', String(32))
    version=Column('version', Boolean)
    metadata_hash=Column('metadata_hash', String(36), unique=True, index=True)
    metadata=Column('metadata', JSON)
    creation_date = Column('creation_date', Date)

class User(Base):
    __tablename__ = 'user'
    row_id=Column('row_id',Integer, primary_key=True, autoincrement=True)
    user_id = Column('user_id', Integer, unique=True, index=True)
    username=Column('username', String(100), unique=True, index=True)
    password=Column('password', String(100))
    study_name=Column('study_name', Integer)
    token = Column('token', Text)
    token_issued = Column('token_issued', Date)
    token_expiry = Column('token_expiry', Date)
    user_role = Column('user_role', String(56))
    user_metadata=Column('user_metadata', JSON)
    user_settings = Column('user_settings', JSON)
    active = Column('active', Boolean)
    creation_date = Column('creation_date', Date)


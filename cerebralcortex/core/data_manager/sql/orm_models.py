import sqlalchemy as db
from sqlalchemy import Column, String, Integer, Date, Boolean, Numeric, JSON, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Stream(Base):
    __tablename__ = 'stream'
    row_id=Column('row_id',Integer, primary_key=True, autoincrement=True)
    name=Column('name', String(100))
    version=Column('version', Boolean)
    metadata_hash=Column('metadata_hash', String(100), unique=True, index=True)
    stream_metadata=Column('stream_metadata', JSON)
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

class CC_Cache(Base):
    __tablename__ = "cc_cache"
    cache_key = Column("cache_key", String(100), primary_key=True)
    cache_value = Column("cache_value", Text)

class Ingestion_Logs(Base):
    __tablename__ = "ingestion_logs"
    row_id = Column('row_id', Integer, primary_key=True, autoincrement=True)
    user_id = Column('user_id', String(100), index=True)
    stream_name = Column('stream_name', String(100), index=True)
    stream_metadata = Column('stream_metadata', JSON)
    platform_metadata = Column('platform_metadata', JSON)
    file_path = Column('file_path', String(100))
    fault_type = Column('fault_type', String(100))
    fault_description = Column('fault_description', String(200))
    success = Column('success', Integer)
    added_date = Column('added_date', Date)

engine = db.create_engine('mysql+mysqlconnector://root:pass@localhost:3306')
engine.execute("CREATE DATABASE IF NOT EXISTS cerebralcortex")
engine.execute("USE cerebralcortex")
Base.metadata.create_all(engine)

Session = db.orm.sessionmaker()
Session.configure(bind=engine)
session = Session()
session.add()
connection = engine.connect()
metadata = db.MetaData()
print(metadata)
import os
import sys
from sqlalchemy import Column, Boolean, Integer, DateTime, Float, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sql_session_maker

from pyhocon import ConfigFactory

if not os.environ['DWAPI_CONF']:
    print("$DWAPI_CONF is not in environment variable")
    sys.exit(1)

appcfg = ConfigFactory.parse_file(os.environ['DWAPI_CONF'])

engine = create_engine(appcfg['database.engine'], echo=appcfg['database.echo'])
Base = declarative_base()

Session = sql_session_maker()
Session.configure(bind=engine)
db_session = Session()

class ApiUsage(Base):
    __tablename__ = 'api_usage'

    id = Column(Integer, primary_key=True)
    srv = Column(String)
    uid = Column(String)
    status = Column(Integer)
    ctime = Column(DateTime)  # request timestamp
    db = Column(String)
    tab = Column(String)
    hit_cache = Column(Boolean)
    elapsed = Column(Float)
    uri = Column(String)  # query string
    msg = Column(Text)

    def __repr__(self):
        return "<ApiUsage(uid='%s', at='%s', uri='%s')>" % (self.uid, self.ctime, self.uri)

def init_db():
    Base.metadata.create_all(engine)

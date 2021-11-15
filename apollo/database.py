import urllib
from sqlalchemy import create_engine, event
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from apollo.main import site_settings

###############################
# /!\ WARNING
###############################

def create_spot_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=SQLDB;UID=" + site_settings.SQLUSR + ";PWD=" + site_settings.SQLPWD + ";")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    return engine

###############################
# /!\ WARNING
###############################

Base = declarative_base()

def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute('pragma foreign_keys=ON')

apollo_engine: Engine = create_engine('sqlite:///{}'.format(site_settings.DB_LOCATION), connect_args={"check_same_thread": False})

event.listen(apollo_engine, 'connect', _fk_pragma_on_connect)

ApolloSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind = apollo_engine)
import pandas as pd
import json
import pdb
import yaml
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

# Variables de ambiente
load_dotenv(find_dotenv())


def get_engine():
    """
    Get SQLalchemy engine using credentials.
    Input stored in .env
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT")

    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    engine = create_engine(url, poolclass=NullPool)
    return engine

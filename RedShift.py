from configparser import ConfigParser
import pandas as pd
import sqlalchemy as sa


def build_conn_string(config_path, config_section):

    """Construye la cadena de conexion a la base de datos a partir del archivo de config"""

    parser = ConfigParser()
    parser.read(config_path)

    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    conn_string = f'Redshift://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string

build_conn_string('config/config.ini','Redshift')

def connect_to_db(conn_string):
    #crea la conexion a la base de datos Redshift
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine





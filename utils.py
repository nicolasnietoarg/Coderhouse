import json
import pandas as pd
from binance.client import Client
from configparser import ConfigParser
from datetime import datetime
import sqlalchemy as sa


def obtener_klines_binance(symbol, interval):
    # Lee las credenciales desde el archivo de configuración
    parser = ConfigParser()
    parser.read("config/config.ini")
    config = parser["API"]
    API_KEY = config["API_KEY"]
    API_SK = config["API_SK"]

    # Crea el cliente de Binance
    client = Client(API_KEY, API_SK)

    # Obtiene los Klines
    klines = client.futures_klines(symbol=symbol, interval=interval)

    # Procesa los datos
    data = {"data": []}

    for kline in klines:
        timestamp = kline[0]
        fecha_hora = datetime.fromtimestamp(timestamp / 1000)
        data["data"].append(
            {
                "timestamp": fecha_hora.strftime("%Y-%m-%d %H:%M:%S"),
                "apertura": kline[1],
                "high": kline[2],
                "low": kline[3],
                "close": kline[4],
                "volume": kline[5],
            }
        )

    # Convierte los datos a un DataFrame
    json_data = json.loads(json.dumps(data))
    df = pd.json_normalize(json_data["data"])

    return df


def build_conn_string(config_path, config_section):

    """Construye la cadena de conexion a la base de datos a partir del archivo de config"""

    parser = ConfigParser()
    parser.read(config_path)

    config = parser[config_section]
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    username = config["username"]
    pwd = config["pwd"]

    conn_string = (
        f"postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require"
    )

    return conn_string


def connect_to_db(conn_string):
    # crea la conexion a la base de datos Redshift
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


def load_data_redshift(df, tabla, conn_string):
    # conectar a Redshift
    conn, engine = connect_to_db(conn_string)

    try:
        # limitador de registros para la carga
        df_subset = df.head(100)

        # carga los datos en la tabla de redshift
        df_subset.to_sql(
            tabla,
            engine,
            if_exists="replace",
            index=False,
            dtype={
                "timestamp": sa.types.TEXT,
                '"open"': sa.types.TEXT,
                "high": sa.types.TEXT,
                "low": sa.types.TEXT,
                "close": sa.types.TEXT,
                "volume": sa.types.TEXT,
            },
        )
        print(
            f"He realizado la carga de los primero 100 registros en la {tabla}. Alto laburo."
        )
    except Exception as e:
        print(f"Hay algo mal que no esta bien en la {tabla}: {str(e)}")
    finally:
        # conexion finalizada
        conn.close()

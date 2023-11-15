from utils import *
from configparser import ConfigParser

parser = ConfigParser()
parser.read('config/config.ini')

symbol = "BTCUSDT"
intervalo = "1h"
tabla = 'binance_data'

df_klines = obtener_klines_binance(symbol, intervalo)

conn_string = build_conn_string('config/config.ini', 'Redshift')

load_data_redshift(df_klines, tabla, conn_string)

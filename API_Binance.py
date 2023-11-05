import json
import pandas as pd
from binance.client import Client
import config

client = Client(config.API_KEY, config.API_SK)

symbol = "BTCUSDT"
interval = "1h"

klines = client.futures_klines(symbol=symbol, interval=interval)

data = {
    "data": []
}

import datetime

for kline in klines:
    timestamp = kline[0]
    fecha_hora = datetime.datetime.fromtimestamp(timestamp / 1000)
    data["data"].append({
        "timestamp": fecha_hora.strftime("%Y-%m-%d %H:%M:%S"),
        "open": kline[1],
        "high": kline[2],
        "low": kline[3],
        "close": kline[4],
        "volume": kline[5]
    })



json_data = json.loads(json.dumps(data))
df = pd.json_normalize(json_data["data"])
df = df.head()
print(df)
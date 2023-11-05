/*QUERY PARA CREAR LA TABLA EN SQL*/
CREATE TABLE IF NOT EXISTS nicolasnietoarg_coderhouse.binance_data (
    "timestamp" TIMESTAMP null,
    "open" FLOAT null,
    "high" FLOAT null,
    "low" FLOAT null,
    "close" FLOAT null,
    "volume" FLOAT null
);

-- la tabla no contiene sortkey por el momento porque aun no esta asignado el simbolo
-- el proceso toma la informacion y la lleva a un staging.
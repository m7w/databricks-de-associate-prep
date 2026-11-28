import sys
from datetime import date
import requests
from pyspark.sql import SparkSession, Row
from delta.tables import DeltaTable

catalog, schema, secret_scope = sys.argv[1:4]

spark = SparkSession.builder.getOrCreate()
api_key = dbutils.secrets.get(scope=secret_scope, key="currencybeacon_api_key")
today_str = date.today().isoformat()

resp = requests.get(
    "https://api.currencybeacon.com/v1/latest",
    params={"api_key": api_key, "base": "BYN", "symbols": "USD,EUR"},
    timeout=15,
)
resp.raise_for_status()
rates_data = resp.json()["response"]["rates"]

rows = [
    Row(rate_date=today_str, currency=curr, rate=float(val))
    for curr, val in rates_data.items()
]

df_new = spark.createDataFrame(rows)
target_table = f"{catalog}.{schema}.currency_rates"

if not spark.catalog.tableExists(target_table):
    df_new.write.format("delta").saveAsTable(target_table)
else:
    (
        DeltaTable.forName(spark, target_table)
        .alias("t")
        .merge(
            df_new.alias("s"), "t.rate_date = s.rate_date AND t.currency = s.currency"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

from pyspark import pipelines as dp
from pyspark.sql import Row
from pyspark.sql.functions import col, from_json

SCHEMA = (
    "event_id string, customer_id string, amount double, currency string, date string"
)


@dp.table(name="raw_events")
@dp.expect_or_drop("valid_currency", "currency IN ('USD', 'BYN')")
def raw_events():
    username = dbutils.secrets.get(scope="kafka_scope", key="kafka_user")
    password = dbutils.secrets.get(scope="kafka_scope", key="kafka_password")
    kafka_jaas_config = (
        f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule "
        f'required username="{username}" password="{password}";'
    )

    kafka_options = {
        "kafka.bootstrap.servers": spark.conf.get("kafka_bootstrap_servers"),
        "subscribe": spark.conf.get("kafka_topic"),
        "startingOffsets": "earliest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-256",
        "kafka.sasl.jaas.config": kafka_jaas_config,
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.location": spark.conf.get("kafka_ca_location"),
    }

    return (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .select(
            from_json(col("value").cast("string"), SCHEMA).alias("parsed_data"),
            col("topic"),
            col("timestamp"),
        )
        .select("parsed_data.*", "topic", "timestamp")
    )


@dp.materialized_view(name="currency_rates")
def currency_rates():
    return spark.createDataFrame(
        [Row(currency="USD", rate=3.04), Row(currency="BYN", rate=1.0)]
    )


@dp.table(name="enriched_events")
def enriched_events():
    events = spark.readStream.table("raw_events")
    rates = spark.read.table("currency_rates")

    return events.join(rates, on="currency", how="left").select(
        events.event_id,
        events.customer_id,
        (events.amount * rates.rate).alias("amount_byn"),
        events.date,
    )

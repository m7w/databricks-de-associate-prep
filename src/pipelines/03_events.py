from pyspark import pipelines as dp
import pyspark.sql.functions as F

SCHEMA = (
    "event_id string, customer_id string, amount double, currency string, date string"
)


@dp.table(name="raw_events")
@dp.expect_or_drop("valid_currency", "currency IN ('USD', 'BYN')")
def raw_events():
    secret_scope = spark.conf.get("secret_scope")
    username = dbutils.secrets.get(scope=secret_scope, key="kafka_user")
    password = dbutils.secrets.get(scope=secret_scope, key="kafka_password")
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
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("parsed_data"),
            F.col("topic"),
            F.col("timestamp"),
        )
        .select("parsed_data.*", "topic", "timestamp")
    )


@dp.table(name="enriched_events")
def enriched_events():
    events = spark.readStream.table("raw_events")
    rates = spark.read.table("currency_rates")

    return events.join(
        rates,
        on=(events.currency == rates.currency)
        & (F.to_date(events.date).cast("string") == rates.rate_date),
        how="left",
    ).select(
        events.event_id,
        events.customer_id,
        F.round(events.amount / F.coalesce(rates.rate, F.lit(1.0)), 2).alias(
            "amount_byn"
        ),
        events.date,
    )

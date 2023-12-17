# Databricks notebook source
from pyspark.sql import functions as F
from Validation import validate_kafka_dataframe
from pyspark.sql import SparkSession


class KafkaClient:
    spark = SparkSession.builder.appName("Kafka_spark_integration").getOrCreate()

    def __init__(self, bootstrap_server: str, username: str, password: str):
        self._bootstrap_server = bootstrap_server
        self._security_protocol = "SASL_SSL"
        self._listener_name = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required"
        self._sasl_jaas_config = f"{self._listener_name} username='{username}' password='{password}';"
        self._endpoint_identification_algorithm = "HTTPS"
        self._sasl_mechanism = "PLAIN"
    
    
    def read_stream(self, topics: list, **args):
        try:
            topics = ",".join(topics)
            kafka_read_stream_df = (spark.readStream.format("kafka")
                                                    .option("kafka.bootstrap.servers", obj._bootstrap_server)
                                                    .option("kafka.security.protocol", obj._security_protocol)
                                                    .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                                                    .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                                                    .option("kafka.sasl.mechanism", self._sasl_mechansim)
                                                    .options(**args)
                                                    .load()
                                                    .withColumn("value",F.col("value").cast("string")).withColumn("key",F.col("key").cast("string")))
            return kafka_read_stream_df
        except:
            # logging
            raise
    
    def read_batch(self, topics: list, **args):
        try:
            topics = ",".join(topics)
            kafka_read_df = (spark.read.format("kafka")
                                                    .option("kafka.bootstrap.servers", obj._bootstrap_server)
                                                    .option("kafka.security.protocol", obj._security_protocol)
                                                    .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                                                    .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                                                    .option("kafka.sasl.mechanism", self._sasl_mechansim)
                                                    .option("subscribe", topics)
                                                    .options(**args)
                                                    .load()
                                                    .withColumn("value",F.col("value").cast("string")).withColumn("key",F.col("key").cast("string")))

            return kafka_read_df
        except:
            # logging
            raise

    def write_stream(self, kafka_stream_df, topic_name, **args):
        try:
            # verify columns and type
            kafka_stream_df = validate_kafka_dataframe(kafka_stream_df)
            kafka_write_stream = (kafka_stream_df.writeStream.format("kafka")
                                                    .option("kafka.bootstrap.servers", obj._bootstrap_server)
                                                    .option("kafka.security.protocol", obj._security_protocol)
                                                    .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                                                    .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                                                    .option("kafka.sasl.mechanism", self._sasl_mechansim)
                                                    .option("topic",topic_name)
                                                    .options(**args)
                                                    .start())
            return kafka_write_stream
        except:
            # logging
            raise

    def write_batch(self, kafka_df, **args):
        try:
            # verify columns and type
            kafka_df = validate_kafka_dataframe(kafka_df)

            (kafka_df.write.format("kafka")
                            .option("kafka.bootstrap.servers", obj._bootstrap_server)
                            .option("kafka.security.protocol", obj._security_protocol)
                            .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                            .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                            .option("topic",topic_name)
                            .option("kafka.sasl.mechanism", self._sasl_mechansim)
                            .option("topic",topic_name)
                            .options(**args)
                            .save())
            return True
        except:
            # logging
            raise


# COMMAND ----------


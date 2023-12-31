from pyspark.sql import functions as F
from Validation import validate_kafka_dataframe
from pyspark.sql import SparkSession
from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()

spark = SparkSession.builder.appName("KafkaClient").getOrCreate()

class KafkaClient:
    """
    This class provide functionality to connect apache kafka
    with apache spark by providing read and write methods for both batch & stream operations.
    """

    def __init__(self, bootstrap_server: str, username: str, password: str):
        self._bootstrap_server = bootstrap_server
        self._security_protocol = "SASL_SSL"
        self._listener_name = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required"
        self._sasl_jaas_config = f"{self._listener_name} username='{username}' password='{password}';"
        self._endpoint_identification_algorithm = "HTTPS"
        self._sasl_mechanism = "PLAIN"
    
    
    def read_stream(self, topics: list, **args):
        """
        returns spark streaming dataframe.
        params:
            topics: list
                list of topics to read.
            args:  dict
                optional configuration to provide while reading kafka stream
        returns:
            spark streaming dataframe
        """
        try:
            topics = ",".join(topics)
            kafka_read_stream_df = (spark.readStream.format("kafka")
                                                    .option("kafka.bootstrap.servers", self._bootstrap_server)
                                                    .option("kafka.security.protocol", self._security_protocol)
                                                    .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                                                    .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                                                    .option("kafka.sasl.mechanism", self._sasl_mechansim)
                                                    .options(**args)
                                                    .load()
                                                    .withColumn("value",F.col("value").cast("string")).withColumn("key",F.col("key").cast("string")))
            return kafka_read_stream_df
        except Exception as e:
            logger.error(f"Exception occured while reading stream for topics {topics}")
            raise
    
    def read_batch(self, topics: list, **args):
        """
        returns spark batch dataframe.
        params:
            topics: list
                list of topics to read.
            args:  dict
                optional configuration to provide while reading kafka stream
        returns:
            spark batch dataframe
        """ 
        try:
            topics = ",".join(topics)
            kafka_read_df = (spark.read.format("kafka")
                                                    .option("kafka.bootstrap.servers", self._bootstrap_server)
                                                    .option("kafka.security.protocol", self._security_protocol)
                                                    .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                                                    .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                                                    .option("kafka.sasl.mechanism", self._sasl_mechansim)
                                                    .option("subscribe", topics)
                                                    .options(**args)
                                                    .load()
                                                    .withColumn("value",F.col("value").cast("string")).withColumn("key",F.col("key").cast("string")))

            return kafka_read_df
        except Exception as e:
            logger.error(f"Exception occured while reading topics {topics}")

            raise

    def write_stream(self, kafka_stream_df, topic_name, **args):
        """
        writes spark stream into kafka.
        params:
            kafka_stream_df: spark streaming dataframe
            topic_name:  string
            args: dict:
                optional configuration to provide while writing kafka stream
        returns:
            spark streaming object
        """
        try:
            # verify columns and type
            kafka_stream_df = validate_kafka_dataframe(kafka_stream_df)
            kafka_write_stream = (kafka_stream_df.writeStream.format("kafka")
                                                    .option("kafka.bootstrap.servers", self._bootstrap_server)
                                                    .option("kafka.security.protocol", self._security_protocol)
                                                    .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                                                    .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                                                    .option("kafka.sasl.mechanism", self._sasl_mechansim)
                                                    .option("topic",topic_name)
                                                    .options(**args)
                                                    .start())
            logger.info("kafka write stream started")
            return kafka_write_stream
        except:
            logger.error("kafka write stream failed")
            raise

    def write_batch(self, kafka_df,topic_name, **args):
        """
        writes spark dataframe into kafka.
        params:
            kafka_df: spark batch dataframe
            topic_name:  string
            args: dict:
                optional configuration to provide while writing kafka stream
        returns:
            bool
        """
        try:
            # verify columns and type
            kafka_df = validate_kafka_dataframe(kafka_df)

            (kafka_df.write.format("kafka")
                            .option("kafka.bootstrap.servers", self._bootstrap_server)
                            .option("kafka.security.protocol", self._security_protocol)
                            .option("kafka.sasl.jaas.config", self._sasl_jaas_config)
                            .option("kafka.ssl.endpoint.identification.algorithm", self._endpoint_identification_algorithm)
                            .option("topic",topic_name)
                            .option("kafka.sasl.mechanism", self._sasl_mechansim)
                            .option("topic",topic_name)
                            .options(**args)
                            .save())
            logger.info("kafka write successful")
            
            return True
        except:
            logger.info("kafka write failed")
            raise



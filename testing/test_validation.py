from pyspark.sql import SparkSession
from utils.Validation import validate_kafka_dataframe
import pytest

spark = SparkSession.builder.appName("KafkaClient").getOrCreate()

def test_validate_kafka_dataframe():
    data = [("1","{'values':'123'}"),("2","{'values':'abc'}")]
    schema_ddl = "key int, value string"

    test_df = spark.createDataFrame(data,schema_ddl)
    validated_test_df = validate_kafka_dataframe(test_df)

    assert test_df.count() == validated_test_df.count()

def test_invalidate_kafka_dataframe():
    data = [("1","{'values':'123'}"),("2","{'values':'abc'}")]
    schema_ddl = "key int, val string"

    test_df = spark.createDataFrame(data,schema_ddl)
    with pytest.raises(AssertionError):
        validate_kafka_dataframe(test_df)
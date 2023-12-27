from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
import sys
import json

from utils.secrets import AzureSecret
from utils.KafkaClient import KafkaClient
from utils.Adls import Adls
from utils.StructSchema import rating_playstore_schema,fetch_schema
from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()


    

def main():
    spark = SparkSession.builder.appName("Kafka_spark_integration").getOrCreate()

    args = sys.argv
    keyvault_args = json.loads(args[0])
    adls_args = json.loads(args[1])
    job_args = json.loads(args[2])


    key_vault_url = keyvault_args["url"]
    tenant_id = keyvault_args["tenant_id"]
    client_id = keyvault_args["client_id"]
    client_secret = keyvault_args["client_secret"]

    adls_account_name = adls_args["account_name"]
    adls_container_name = adls_args["container_name"]
    adls_tenant_id = adls_args["tenant_id"]

    azure_secret_obj = AzureSecret(key_vault_url,tenant_id,client_id,client_secret)

    kafka_username = azure_secret_obj.get_secrets("kafka_username")
    kafka_password = azure_secret_obj.get_secrets("kafka_username")
    adls_client_id = azure_secret_obj.get_secrets("adls_client_id")
    adls_client_secret = azure_secret_obj.get_secrets("adls_client_secret")

    adls_obj = Adls(adls_account_name,adls_container_name,adls_tenant_id,adls_client_id,adls_client_secret)
    path = adls_obj.get_path()

    kafka_bootstrap_server = job_args["bootstrap_server"]

    kafka_client_obj = KafkaClient(kafka_bootstrap_server,kafka_username,kafka_password)
    topic_name = job_args["topic_list"]
    checkpointLocation = path + "/" + job_args["checkpointLocation"]
    table_name = job_args["table_name"]
    schema_name = job_args["schema_name"]
    schema = fetch_schema(schema_name)
    if not schema:
        logger.info(f"schema not found {schema_name}")

    kafka_stream_df = kafka_client_obj.read_stream(topic_name)

    def write_to_delta_lake(batch_df, batch_id):
        flatten_df = batch_df.withColumn("value",F.from_json("value",schema)).select("key","value.*")
        final_df = flatten_df.withColumn("current_timestamp",F.current_timestamp())
        final_df.write.format("delta").mode("append").save(f"{path}/{table_name}")

    logger.info(f"stream started, writing into delta lake {table_name}")
    kafka_stream_obj = kafka_stream_df.writeStream.trigger(processingTime="10 seconds").option("checkpointLocation",checkpointLocation).foreachbatch(write_to_delta_lake).start()

    flag = kafka_stream_obj.awaitTermination(3600) # 1 hour time
    if not flag:
        kafka_stream_obj.stop()
        logger.info("stream stopped forcefully")


if __name__ == "main":
    main()



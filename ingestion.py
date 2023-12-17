# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

from utils.secrets import AzureSecret
from utils.KafkaClient import KafkaClient
from utils.Adls import Adls
from utils.StructSchema import rating_playstore_schema

def write_to_delta_lake(batch_df, batch_id):
    global path
    flatten_df = batch_df.withColumn("value",F.from_json("value",rating_playstore_schema)).select("key","value.*")
    hashed_df = flatten_df.withColumn("author_name",F.sha2("author_name",256)).withColumn("review_text",F.sha2("review_text",256))
    final_df = hashed_df.withColumn("review_timestamp",F.to_timestamp("review_timestamp","dd-MM-yyyy HH:mm:ss a")).withColumn("current_timestamp",F.current_timestamp())
    final_df.write.format("delta").mode("append").save(f"{path}/rating_playstore")
    

def main():
    spark = SparkSession.builder.appName("Kafka_spark_integration").getOrCreate()

    key_vault_url = "https://<your-key-vault-name>.vault.azure.net/"
    tenant_id = "<your-tenant-id>"
    client_id = "<your-client-id>"
    client_secret = "<your-client-secret>"

    adls_account_name = "<your-adls-account-name>"
    adls_container_name = "<your-container-name>"
    adls_tenant_id = "<your-tenant-id>"

    azure_secret_obj = AzureSecret(key_vault_url,tenant_id,client_id,client_secret)

    kafka_username = azure_secret_obj.get_secrets("kafka_username")
    kafka_password = azure_secret_obj.get_secrets("kafka_username")
    adls_client_id = azure_secret_obj.get_secrets("adls_client_id")
    adls_client_secret = azure_secret_obj.get_secrets("adls_client_secret")

    adls_obj = Adls(adls_account_name,adls_container_name,adls_tenant_id,adls_client_id,adls_client_secret)
    path = adls_obj.get_path()

    kafka_bootstrap_server = "pkc-7prvp.centralindia.azure.confluent.cloud:9092"

    kafka_client_obj = KafkaClient(kafka_bootstrap_server,kafka_username,kafka_password)
    topic_name = ["rating_playstore"]
    kafka_stream_df = kafka_client_obj.read_stream(topic_name)

    kafka_stream_obj = kafka_stream_df.writeStream.trigger(processingTime="10 seconds").option("checkpointLocation",f"{path}/rating_playstore_checkpoint/").foreachbatch(write_to_delta_lake).start()

    flag = kafka_stream_obj.awaitTermination(3600) # 1 hour time
    if not flag:
        kafka_stream_obj.stop()

    # loggings required each step
if __name__ == "main":
    main()













# COMMAND ----------


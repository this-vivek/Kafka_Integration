from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import *

from utils.secrets import AzureSecret
from utils.Adls import Adls
from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()


def main():
    spark = SparkSession.builder.appName("Kafka_spark_integration_process_and_transform").getOrCreate()

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
    playstore_rating_raw_path = f"{path}/rating_playstore"
    playstore_rating_silver_path = f"{path}/rating_playstore_silver"
    
    logger.info("silver processing started, writing into delta lake")


    raw_playstore_ratings_df  = spark.read.load(playstore_rating_raw_path)
    allowed_characters = 'A-Za-z0-9\s' 
    pattern = f'[^{allowed_characters}]'
    raw_playstore_ratings_valid_review_df = raw_playstore_ratings_df.withColumn("review_text",
                                                                                F.regexp_replace(F.col("review_text"),pattern,""))
    window_spec_id = Window.partitionBy("pseudo_author_id")
    raw_playstore_ratings_latest_df = raw_playstore_ratings_valid_review_df.withColumn("rn",F.row_number()
                                                                                       .over(window_spec_id.orderBy(
                                                                                           F.desc("review_timestamp")))) \
                                                                           .filter("rn == 1") \
                                                                           .drop("rn")

    # More silver level transformation - cleansing, validation, filteration and etc.

    raw_playstore_ratings_final_df = raw_playstore_ratings_latest_df
    exist_flag = Adls.check_if_exists(playstore_rating_silver_path)

    if not exist_flag:
        logger.info("writing for the first time")

        raw_playstore_ratings_final_df.write.format("delta").save(playstore_rating_silver_path)
        logger.info("Silver processing ended")


        return
    
    deltaTablePeople = DeltaTable.forPath(spark, playstore_rating_silver_path)
    deltaTablePeople.alias('silver') \
    .merge(
        raw_playstore_ratings_final_df.alias('bronze'),
        'silver.pseudo_author_id = bronze.pseudo_author_id'
    ) \
    .whenMatchedUpdate(set =
        {
        "pseudo_author_id": "bronze.pseudo_author_id",
        "author_name": "bronze.author_name",
        "review_text": "bronze.review_text",
        "review_rating": "bronze.review_rating",
        "review_likes": "bronze.review_likes",
        "author_app_version": "bronze.author_app_version",
        "review_timestamp": "bronze.review_timestamp",
        "current_timestamp": F.current_timestamp()
        }
    ) \
    .whenNotMatchedInsert(values =
        {
        "pseudo_author_id": "bronze.pseudo_author_id",
        "author_name": "bronze.author_name",
        "review_text": "bronze.review_text",
        "review_rating": "bronze.review_rating",
        "review_likes": "bronze.review_likes",
        "author_app_version": "bronze.author_app_version",
        "review_timestamp": "bronze.review_timestamp",
        "current_timestamp": F.current_timestamp()
        }
    ) \
    .execute()

    logger.info("Silver processing ended")
    
    

if __name__ == "main":
    main()

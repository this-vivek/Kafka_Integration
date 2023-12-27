from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import *
import sys
import json

from utils.secrets import AzureSecret
from utils.Adls import Adls
from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()

def prepare_merge_statement_dict(df,key_column_list):
    """
    This function prepare joining_condition and upsert dictionary for merge statement.
    Params:
        df: Spark Dataframe
            dataframe to be written
        key_column_list: List
            list of key columns in silver table
        
    returns:
        joining_condition,update_and_insert_col_dict
    """
    joining_condition = " and ".join([f"bronze.{i} = silver.{i}" for i in key_column_list])

    update_and_insert_cols_dict = {column_name:f"bronze.{column_name}" for column_name in df.columns}
    update_and_insert_cols_dict["current_timestamp"] = F.current_timestamp()

    return joining_condition, update_and_insert_cols_dict


def main():
    spark = SparkSession.builder.appName("Kafka_spark_integration_process_and_transform").getOrCreate()

    # Required arguments can be passed via any orchestration tool.
    # Receiving json strings as input for each category of requirements.
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

    # Fetching azure client secrets using AzureSecrets Module
    adls_client_id = azure_secret_obj.get_secrets("adls_client_id")
    adls_client_secret = azure_secret_obj.get_secrets("adls_client_secret")

    adls_obj = Adls(adls_account_name,adls_container_name,adls_tenant_id,adls_client_id,adls_client_secret)
    path = adls_obj.get_path()

    raw_path = job_args["raw_path"]
    silver_path = job_args["silver_path"]
    silver_table_name = job_args["silver_table_name"]
    raw_key_column = job_args["key_column_list"]
    raw_order_column = job_args["order_column_list"]

    
    logger.info(f"silver processing started, writing into delta lake {silver_table_name}")


    raw_df  = spark.read.load(raw_path)

    # Parsing latest records for each key to prepare SCD 1 Table
    window_spec_id = Window.partitionBy(*raw_key_column)
    raw_latest_df = raw_df.withColumn("rn",F.row_number().over(window_spec_id.orderBy(F.desc(*raw_order_column)))) \
                                                                           .filter("rn == 1") \
                                                                           .drop("rn")

    # More silver level transformation - cleansing, validation, filteration and etc.

    raw_final_df = raw_latest_df
    exist_flag = Adls.check_if_exists(silver_path)

    logger.info(f"writing for the first time {silver_table_name}")

    if not exist_flag:
        raw_final_df.write.format("delta").save(silver_path)
        logger.info("Silver processing ended")
        return True
    
    joining_condition, update_and_insert_cols_dict = prepare_merge_statement_dict(raw_final_df, raw_key_column)

    deltaTablePeople = DeltaTable.forPath(spark, silver_path)

    # Performing merge operation between bronze and silver table
    deltaTablePeople.alias('silver') \
    .merge(
        raw_final_df.alias('bronze'),
        joining_condition
    ) \
    .whenMatchedUpdate(set = update_and_insert_cols_dict
    ) \
    .whenNotMatchedInsert(values = update_and_insert_cols_dict
    ) \
    .execute()

    logger.info("Silver processing ended")
    return True
    

if __name__ == "main":
    main()

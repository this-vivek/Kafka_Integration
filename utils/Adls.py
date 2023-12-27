# Databricks notebook source
from pyspark.sql import SparkSession
from utils.logging import LogGenerator

spark = SparkSession.builder.appName("KafkaClient").getOrCreate()
logger = LogGenerator().GetLogger()

class Adls:
    def __init__(self, account_name, container_name, tenant_id, client_id,client_secret):
        try:
            spark.conf.set(
                f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net",
                "OAuth",
            )
            spark.conf.set(
                f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            )
            spark.conf.set(
                f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net",
                client_id,
            )
            spark.conf.set(
                f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net",
                client_secret,
            )
            spark.conf.set(
                f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net",
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
            )

            self.path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net"
            
            logger.info("ADLS Config Successful")
        except:
            logger.error("ADLS Config Failed")
            raise
    
    def get_path(self):
        return self.path
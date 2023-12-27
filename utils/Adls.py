# Databricks notebook source
from pyspark.sql import SparkSession
from utils.logging import LogGenerator
import os

spark = SparkSession.builder.appName("KafkaClient").getOrCreate()
logger = LogGenerator().GetLogger()

class Adls:
    """
    ADLS class sets spark configuration for connecting Spark env with ADLS gen2.
    Methods:
        1) get_path:
            returns base_path for connected adls.
        2) check_if_exists:
            params:
                adls_path
            returns boolean based of status.
    """

    def __init__(self, account_name, container_name, tenant_id, client_id,client_secret):
        """
        params:
            account_name: String
                Name of the storage account.
            container_name: String
                Name of the storage account conatainer.
            Tenant_id: String
                Subscription Tenant ID.
            Client_id: String
                Subscription Client ID.
            Client_secret: String
                Subscription Client Secret.
        """
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
    
    def check_if_exists(self,adls_path):
        exists = os.path.exists(adls_path)
        return exists

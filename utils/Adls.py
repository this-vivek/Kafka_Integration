# Databricks notebook source
class Adls:
    def __init__(self, account_name, container_name, tenant_id, client_id,client_secret):
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
    
    def get_path(self):
        return self.path
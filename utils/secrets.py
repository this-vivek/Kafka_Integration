# Databricks notebook source
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError

class AzureSecret:

    def __init__(self,key_vault_url: str,tenant_id: str, client_id: str, client_secret: str ):
        self._credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self._secret_client = SecretClient(vault_url=key_vault_url, credential=self._credential)

        self.secret_names = ["kafka_secret","adls_secret"]

    def get_secrets(self,secret_name):
        try:
            # Try to get the secret from Azure Key Vault
            secret = self._secret_client.get_secret(secret_name)
            return secret.value

        except ResourceNotFoundError:
            print(f"Secret '{secret_name}' not found in Azure Key Vault.")
        except Exception as e:
            print(f"An error occurred: {e}")


# COMMAND ----------


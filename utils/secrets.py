from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError
from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()

class AzureSecret:
    """
    This class takes provide accessiblity to Azure Secrets using Azure Modules.
    """
    def __init__(self,key_vault_url: str,tenant_id: str, client_id: str, client_secret: str ):
        self._credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self._secret_client = SecretClient(vault_url=key_vault_url, credential=self._credential)

        self.secret_names = ["kafka_secret","adls_secret"]

    def get_secrets(self,secret_name):
        """
        returns azure secret value.
        """
        try:
            # Try to get the secret from Azure Key Vault
            secret = self._secret_client.get_secret(secret_name)
            return secret.value

        except ResourceNotFoundError:
            logger.error(f"Secret '{secret_name}' not found in Azure Key Vault.")
            raise
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise




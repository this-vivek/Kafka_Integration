import unittest
from unittest.mock import patch, Mock
from ..utils.secrets import AzureSecret

class TestAzureSecret(unittest.TestCase):

    @patch('azure.identity.ClientSecretCredential')
    @patch('azure.keyvault.secrets.SecretClient')
    @patch('utils.logging.LogGenerator')
    def test_get_secrets(self, mock_log_generator, mock_secret_client, mock_credential):
        # Mock the logger
        mock_logger = Mock()
        mock_log_generator.return_value.GetLogger.return_value = mock_logger

        # Mock the Azure Key Vault interactions
        mock_credential.return_value = Mock()
        mock_secret = Mock(value='password')
        mock_secret_client.return_value.get_secret.return_value = mock_secret

        # Create an instance of AzureSecret for testing
        azure_secret = AzureSecret("https://your-key-vault-url", "your-tenant-id", "your-client-id", "your-client-secret")

        # Call the get_secrets method and check if it returns the expected value
        result = azure_secret.get_secrets("kafka_secret")

        # Assert that the result matches the mocked secret value
        self.assertEqual(result, 'password')

        # Assert that the logger was not called with error messages
        mock_logger.error.assert_not_called()


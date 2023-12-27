import subprocess
import sys
import json

from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()

# Following is the dummy args that will be passed as system arguments.

# keyvault_args = {
#     "url": "https://example-keyvault.vault.azure.net",
#     "tenant_id": "your_tenant_id",
#     "client_id": "your_client_id",
#     "client_secret": "your_client_secret"
#   }
# adls_args = {
#     "account_name": "your_adls_account_name",
#     "container_name": "your_adls_container_name",
#     "tenant_id": "your_adls_tenant_id"
#   }
# job_args = {
#     "bootstrap_server": "your_kafka_bootstrap_server",
#     "topic_list": "your_topic_name",
#     "checkpointLocation": "/path/to/checkpoint",
#     "table_name": "your_table_name",
#     "schema_name": "your_schema_name"
#   }
def execute_script(script_path, *args):
    subprocess.run([sys.executable, script_path] + list(args))

def main():
    # Process sys argurments for two jobs ingest and process_and_transform
    sys_args = sys.argv
    ingest_args = json.loads(sys_args[0])
    process_and_transform_args = json.loads(sys_args[1])
    ingestion_script_path = "ingest.py" 
    process_and_transform_script_path = "process_and_transform.py" 

    ingestion_arguments = [json.dumps(ingest_args)] 
    process_and_transform_arguments = [json.dumps(process_and_transform_args)] 

    # Executing ingestion script with required arguments
    ingest_success_flag = execute_script(ingestion_script_path, *ingestion_arguments)

    # In case of failure
    if not ingest_success_flag:
        logger.info("ingestion unsuccessfull, skipping process_and_transformation script")

    # Executing process_and_transform with required arguments
    process_and_transform_success_flag = execute_script(process_and_transform_script_path, *process_and_transform_arguments)

    # In case of Failure
    if not process_and_transform_success_flag:
        logger.info("process_and_transform.py Failed")

if __name__ == "main":
    main()

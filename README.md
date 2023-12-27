# Spark Kafka Integration Project

This project demonstrates the integration between Apache Spark and Apache Kafka. It includes two main files, `ingest.py` and `process_and_transform.py`, which are orchestrated by the `orchestrate.py` file. Additionally, the project includes utility classes such as `ADLS`, `secrets`, `StructSchema`, `Validation`, `KafkaClient`, and `logging`. Unit testing has been implemented to ensure the reliability of the components.

## Table of Contents

- [Overview](#overview)
- [Files and Components](#files-and-components)
- [Usage](#usage)
- [Configuration](#configuration)
- [Unit Testing](#unit-testing)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project aims to showcase the seamless connection between Apache Spark and Apache Kafka. The ingestion and processing components are split into two main files:

- `ingest.py`: Handles the data ingestion from a source to Apache Kafka.
- `process_and_transform.py`: Manages the processing and transformation of data consumed from Kafka.

These components are orchestrated by the `orchestrate.py` file.

### Utility Classes

- `ADLS`: Utility class for handling interactions with Azure Data Lake Storage.
- `secrets`: Class for managing sensitive information securely.
- `StructSchema`: Utility class for defining and handling structured data schemas.
- `Validation`: Class for data validation and integrity checks.
- `KafkaClient`: Wrapper class for interacting with Kafka.
- `logging`: Logging utility for maintaining a comprehensive log of activities.

## Files and Components

- **`ingest.py`**: Responsible for ingesting data into Apache Kafka.
- **`process_and_transform.py`**: Manages the processing and transformation of data consumed from Kafka.
- **`orchestrate.py`**: Orchestrates the execution of `ingest.py` and `process_and_transform.py`.

## Usage

To use this project, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/charlie239-CU/Kafka_Integration.git
   cd Kafka_Integration
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
3. Run the orchestration script:
  ```bash
   python orchestrate.py "{'keyvault_args':'value'}" "{'adls_args':'value'}" "{'job_level_args':'value'}"

Dummy Arguments:
   ```python
   {
     "keyvault_args": {
       "url": "https://example-keyvault.vault.azure.net",
       "tenant_id": "your_tenant_id",
       "client_id": "your_client_id",
       "client_secret": "your_client_secret"
     },
     "adls_args": {
       "account_name": "your_adls_account_name",
       "container_name": "your_adls_container_name",
       "tenant_id": "your_adls_tenant_id"
     },
     "job_args": {
       "bootstrap_server": "your_kafka_bootstrap_server",
       "topic_list": "your_topic_name",
       "checkpointLocation": "/path/to/checkpoint",
       "table_name": "your_table_name",
       "schema_name": "your_schema_name"
     }
   }


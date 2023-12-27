from utils.logging import LogGenerator

logger = LogGenerator().GetLogger()

def validate_kafka_dataframe(kafka_df):
    """
    Provide Validation for kafka write dataframe with sets of assertions.
    params:
        kafka_df: Spark Dataframe
    returns:
        spark dataframe
    """
    kafka_df_dict = {col_name:col_type for col_name,col_type in kafka_df.dtypes}
    required_columns = {"value":{"string","binary"}}
    optional_columns = {"key": {"string","binary"},
                        "headers":{"array"},
                        "topic":{"string"},
                        "partition":{"int","bigint","long"}}
    
    for col_name in required_columns:
        if col_name not in kafka_df_dict.keys():
            logger.error(col_name + " is required column name")
            raise AssertionError(col_name + " is required column name")
        if kafka_df_dict[col_name] not in required_columns[col_name]:
            logger.error(col_name + " having incorrect data type as " + kafka_df_dict[col_name])
            raise AssertionError(col_name + " having incorrect data type as " + kafka_df_dict[col_name])
    
    kafka_df_dict.pop("value")
    
    for col_name in kafka_df_dict:
        if col_name not in optional_columns.keys():
            logger.error(col_name + " invalid column name")
            raise AssertionError(col_name + " invalid column name")
        if kafka_df_dict[col_name] not in optional_columns[col_name]:
            logger.error(col_name + " having incorrect data type as " + kafka_df_dict[col_name])
            raise AssertionError(col_name + " having incorrect data type as " + kafka_df_dict[col_name])

    return kafka_df
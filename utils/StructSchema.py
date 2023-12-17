# Databricks notebook source
from pyspark.sql.types import *


rating_playstore_schema = StructType([
                                StructField("review_id",StringType()),
                                StructField("author_name",StringType()),
                                StructField("review_text",StringType()),
                                StructField("review_rating",IntegerType()),
                                StructField("review_likes",IntegerType()),
                                StructField("author_app_version",DecimalType()),
                                StructField("review_timestamp",StringType())]    
                            )
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from pyspark.sql.functions import col, sum as _sum, round
from pyspark.sql.types import StructType
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to create Spark Session
def create_spark_session(app_name: str = "MyApp") -> SparkSession:
    logging.info(f"Creating Spark session: {app_name}")
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


# Function to load data
def load_data(
    spark: SparkSession, file_path: str, columns: list = None, schema: StructType = None
) -> DataFrame:
    try:
        logging.info(f"Loading data from {file_path}")
        df = spark.read.csv(
            file_path, header=True, schema=schema
        )  # Use provided schema if available
        if columns:
            df = df.select(*columns)
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


# Function to describe data
def describe_data(df: DataFrame) -> pd.DataFrame:
    logging.info("Describing DataFrame")
    # Consider calculating statistics within Spark to avoid out-of-memory errors
    try:
        return df.describe().toPandas()
    except Exception as e:
        logging.error(f"Error describing data: {e}")
        raise


# # Function to perform a simple query
# def simple_query(df: DataFrame, column_name: str) -> DataFrame:
#     logging.info(f"Querying distinct values from {column_name}")
#     try:
#         return df.select(column_name).distinct().count()
#     except Exception as e:
#         logging.error(f"Error performing query: {e}")
#         raise


def top_countries_by_capacity(
    df: DataFrame,
    capacity_column: str = "capacity_mw",
    country_column: str = "country_long",
) -> DataFrame:
    logging.info("Selecting top countries by capacity")
    try:
        # Group by the country column and sum the capacity_mw column
        top_countries = df.groupBy(country_column).agg(
            _sum(capacity_column).alias("total_capacity")
        )

        # Round the total capacity to 2 decimal places
        top_countries = top_countries.withColumn(
            "total_capacity", round(col("total_capacity"), 2)
        )

        # rename country_long to COUNTRY and total_capacity to CAPACITY
        top_countries = top_countries.withColumnRenamed(
            "country_long", "COUNTRY"
        ).withColumnRenamed("total_capacity", "TOTAL CAPACITY")

        # Order by total capacity in descending order and get the top 10
        t_countries = top_countries.orderBy(col("total_capacity").desc()).limit(10)

        # Order by total capacity in ascending order and get the top 10
        b_countries = top_countries.orderBy(col("total_capacity").asc()).limit(10)

        return t_countries, b_countries
    except Exception as e:
        logging.error(f"Error selecting top countries: {e}")
        raise

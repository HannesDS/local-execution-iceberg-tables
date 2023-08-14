from typing import Iterable, Final

from pyspark.sql import DataFrame, SparkSession


def create_iceberg_table_from_dataframe(spark: SparkSession, data: DataFrame, table_name: str,
                                        database_name: str, ):
    data.createOrReplaceTempView("dataframe")
    query: str = construct_create_or_replace_table_query_simple(
        glue_database=database_name,
        glue_table=table_name,
    )
    spark.sql(query)


def table_exists_in_database(
        spark: SparkSession, database: str, table: str
) -> bool:
    spark.sql(f"USE {database}")
    tables_collection = spark.sql(
        "SHOW TABLES"
    ).collect()
    table_names_in_db = [row["tableName"] for row in tables_collection]
    return table in table_names_in_db


def construct_create_or_replace_table_query_simple(
        glue_database: str,
        glue_table: str,
):
    return f"""
        CREATE OR REPLACE TABLE {glue_database}.{glue_table}
        USING ICEBERG
        AS SELECT * from dataframe
        """

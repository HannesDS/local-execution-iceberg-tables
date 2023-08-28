import os
import shutil
import time
import unittest
from functools import lru_cache
from typing import Final
from datetime import datetime
from chispa import assert_approx_df_equality
import pytest as pytest
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from app.iceberg_utils import table_exists_in_database, create_iceberg_table_from_dataframe
from app.windmill_app import WindmillMeasurement, WindMillPowerTable

warehouse_path: str = "./warehouse"
default_database_name: Final[str] = "my_db"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_builder = SparkSession.builder.appName("test_session")
    spark_builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    spark_builder.config(
        f"spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"
    )
    spark_builder.config(f"spark.sql.catalog.local.type", "hadoop")
    spark_builder.config(f"spark.sql.catalog.local.warehouse", warehouse_path)
    spark_builder.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0')
    spark_builder.config("spark.sql.catalog.local.default.write.metadata-flush-after-create", "true")
    spark_builder.config("spark.sql.defaultCatalog", "local")
    spark: SparkSession = spark_builder.getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {default_database_name}")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def cleanup_warehouse(request):
    yield
    # Remove the ./warehouse folder after the class's tests are done
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)


def test_create_iceberg_table_from_dataframe(spark, cleanup_warehouse):
    mock_dataframe = spark.createDataFrame(
        data=[
            ("1", "a",),
            ("1", "b",),
        ],
        schema=StructType(
            [
                StructField("foo", StringType(), False),
                StructField("bar", StringType(), False),
            ]
        ),
    )
    table_name: str = "my_table"
    create_iceberg_table_from_dataframe(spark=spark, data=mock_dataframe, table_name=table_name,
                                        database_name=default_database_name)
    assert table_exists_in_database(table=table_name, database=default_database_name, spark=spark)


class TestWindMillPowerTable:

    @pytest.fixture(scope="function")
    def windmill_power_table(self, spark):
        data = [
            WindmillMeasurement("1", datetime(2023, 1, 1, 0, 0, 0), 1.0),
            WindmillMeasurement("2", datetime(2023, 1, 1, 0, 0, 0), 1.0),
        ]
        windmill_power_table: WindMillPowerTable = WindMillPowerTable(database_name=default_database_name)
        windmill_power_table.merge_windmill_measurements(spark=spark, windmill_measurements=data)
        yield
        spark.sql(f"DROP TABLE {default_database_name}.{WindMillPowerTable.table_name}")

    def test_wind_mill_power_table(cls, spark, cleanup_warehouse, windmill_power_table):
        expected = spark.createDataFrame([
            WindmillMeasurement("1", datetime(2023, 1, 1, 0, 0, 0), 1.0),
            WindmillMeasurement("2", datetime(2023, 1, 1, 0, 0, 0), 1.0),
        ], schema=WindmillMeasurement.schema())
        result: DataFrame = spark.table(WindMillPowerTable.table_name)
        assert_approx_df_equality(result, expected, precision=1e-5, ignore_nullable=True)

    def test_merge_wind_mill_power_table(self, spark, windmill_power_table,cleanup_warehouse):
        data = [
            # Existing measurement but the same timestamp -> not update
            WindmillMeasurement("1", datetime(2023, 1, 1, 0, 0, 0), 2.0),
            # Existing measurement with a newer timestamp -> update
            WindmillMeasurement("2", datetime(2023, 1, 1, 0, 15, 0), 2.0),
            # New Measurement
            WindmillMeasurement("3", datetime(2023, 1, 1, 0, 15, 0), 1.0),
        ]
        expected = spark.createDataFrame([
            WindmillMeasurement("1", datetime(2023, 1, 1, 0, 0, 0), 1.0),
            WindmillMeasurement("2", datetime(2023, 1, 1, 0, 15, 0), 2.0),
            WindmillMeasurement("3", datetime(2023, 1, 1, 0, 15, 0), 1.0),

        ], schema=WindmillMeasurement.schema())
        windmill_power_table: WindMillPowerTable = WindMillPowerTable(database_name=default_database_name)
        windmill_power_table.merge_windmill_measurements(spark=spark, windmill_measurements=data)
        result: DataFrame = spark.table(WindMillPowerTable.table_name)
        assert_approx_df_equality(result, expected, precision=1e-5, ignore_nullable=True)

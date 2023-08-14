from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType, FloatType,
)

from app.iceberg_utils import table_exists_in_database, create_iceberg_table_from_dataframe


@dataclass
class WindmillMeasurement:
    windmill_id: str
    timestamp_start: datetime
    power_generated_kwh: float

    @classmethod
    def schema(cls):
        return StructType([
            StructField("windmill_id", StringType(), False),
            StructField("timestamp_start", TimestampType(), False),
            StructField("power_generated_kwh", FloatType(), False),
        ])


class WindMillPowerTable:
    database_name: str
    table_name: str = "windmill_power_timeseries"

    def __init__(self, database_name: str, ):
        self.database_name = database_name

    def merge_windmill_measurements(self, spark: SparkSession, windmill_measurements: Iterable[WindmillMeasurement]):
        data = spark.createDataFrame(windmill_measurements, schema=WindmillMeasurement.schema())
        if not table_exists_in_database(spark=spark, database=self.database_name, table=self.table_name):
            create_iceberg_table_from_dataframe(data=data, table_name=self.table_name, database_name=self.database_name,
                                                spark=spark)
        else:
            data.createOrReplaceTempView("source")
            spark.sql(
                f"""MERGE INTO {self.database_name}.{self.table_name} target
                    USING (SELECT * FROM source) source
                    ON source.windmill_id = target.windmill_id
                    WHEN MATCHED AND source.timestamp_start > target.timestamp_start THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)



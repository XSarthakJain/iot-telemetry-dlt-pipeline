from pyspark import pipelines as dp
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DoubleType,ArrayType
from pyspark.sql.functions import col,lit,current_timestamp

sensor_schema = StructType([
    StructField("device_id", StringType(), False),
    StructField("device_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("firmware_version", StringType(), True),
    StructField("battery_pct", IntegerType(), True),
    StructField("connectivity", StringType(), True),
    StructField("location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("accuracy_m", IntegerType(), True)]), True),
    StructField("sensor_readings", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("value", DoubleType(), True),  # handles both integer and decimal values (e.g., 78 or 36.4)
            StructField("unit", StringType(), True),
            StructField("ts", StringType(), True)
        ])
    ), True),
    StructField("error_codes", ArrayType(StringType()), True),
    StructField("app_version", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

@dp.table(
    name = "sensor_bronze_table",
    comment = "Bronze table",
    partition_cols=["device_id"]
)
def sensor_bronze():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("mode", "PERMISSIVE").schema(sensor_schema).option("multiline", "true").option("columnNameOfCorruptRecord", "_corrupt_record").option("cloudFiles.schemaEvolutionMode", "rescue").option("rescuedDataColumn","_rescued_data").load("/Volumes/workspace/default/assets/*.json")

    #Add Metadata
    df = df.withColumn("_ingested_at", current_timestamp()).withColumn("_source_file", col("_metadata.file_path")).withColumn("_file_modification_time", col("_metadata.file_modification_time")).withColumn("_batch_id", lit("batch_job_run")) # Optional: placeholder or run tracking tag
    return df





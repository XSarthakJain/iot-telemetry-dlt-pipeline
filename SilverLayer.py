from pyspark import pipelines as dp
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DoubleType,ArrayType
from pyspark.sql.functions import col,lit,current_timestamp,explode,explode_outer,flatten,expr,unix_timestamp,to_timestamp,filter,broadcast,when

late_threshhold_hours = 6
@dp.table
def silverlayer():
    df = spark.read.table("workspace.default.sensor_bronze_table")
    df1 = df.withColumn("sensor_reading",explode_outer(col("sensor_readings"))).select(col("*"),col("sensor_reading.type").alias("sensor_type"),col("sensor_reading.value").alias("sensor_value"),col("sensor_reading.unit").alias("sensor_unit"),col("sensor_reading.ts").alias("sensor_ts"))

    df2 = df1.withColumn("location_df",df1["location"]).select(col("*"),col("location_df.*"))
    df3 = df2.select(col("*"),expr("try_cast(battery_pct AS INT)").alias("battery_percentage"))
    df4 = df3.dropDuplicates(["device_id","sensor_type","sensor_ts"]).select(col("*"))

    #Validate Late Arrival
    df5 = df4.withColumn("leg_time",(unix_timestamp(to_timestamp(col("_ingested_at"))) - unix_timestamp(to_timestamp(col("sensor_ts"))))/3600).withColumn("Is_late_arrival",col("leg_time") > late_threshhold_hours)
    return df5
@dp.materialized_view
def silver_iot_sensor_readings():
    df = spark.read.table("silverlayer")
    df1 = df.select(col("*")).filter(col("Is_late_arrival")==False)
    
    error_code_df = spark.read.table("workspace.default.error_lookup_dim")
    df2 = df1.withColumn("error_code",explode_outer(col("error_codes")))
    df3 = df2.join(broadcast(error_code_df),on="error_code",how="left")

    #Data Quality
    df4 = df3.withColumn("sensor_comment",when((col("sensor_type")=="heart_rate") & (col("sensor_value")>=lit(30)) & (col("sensor_value")<=lit(200)),"Normal").when((col("sensor_type")=="heart_rate"),"sensor glitch").otherwise("Valid for Heart Rate")).filter((col("battery_percentage")>=0) & (col("battery_percentage")<=100))
    df5 = df4.dropna(subset = ["device_id"])
    return df5

@dp.materialized_view
def iot_late_arrivals():
    df = spark.read.table("silverlayer")
    df1 = df.select(col("*")).filter(col("Is_late_arrival")==True)
    return df1    

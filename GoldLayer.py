from pyspark import pipelines as dp
from pyspark.sql.functions import col,avg,lag,minute,to_date,sum,count,unix_timestamp,cast,stddev,lit,expr
from pyspark.sql import Window
@dp.materialized_view
def health_fact():
    df = spark.read.table("silver_iot_sensor_readings")

    window_spec = Window.partitionBy("device_id").orderBy(col("event_timestamp"))
    df1 = df.withColumn("prev_time",lag(col("event_timestamp")).over(window_spec)).withColumn("prev_battery",lag(col("battery_percentage")).over(window_spec)).filter(col("prev_time")==col("event_timestamp"))

    df2 = df1.withColumn("elapsed_time",(unix_timestamp(col("event_timestamp")) - unix_timestamp(col("prev_time"))) / 3600).withColumn("battery_drop",(col("prev_battery")-col("battery_percentage"))).withColumn("drain_hr",(col("battery_drop")/col("elapsed_time"))).withColumn("current_date",col("event_timestamp")).filter((col("elapsed_time") > 0) & (col("drain_hr") >= 0))

    df3 = df2.groupBy(col("device_id"),col("current_date")).agg((sum(col("drain_hr"))/count(col("drain_hr"))).alias("Average_drain_Per_Hours"),count(col("error_codes")).alias("errorcount"),count(col("severity")).alias("high_severity_errors"))

    return df3

@dp.materialized_view
def sensor_annomalies():
    heart_rate_readings = spark.read.table("silver_iot_sensor_readings")
    # Step 1: calculate each device's own mean and std dev of heart rate
    stats = heart_rate_readings.groupBy("device_id").agg(
        avg("sensor_value").alias("mean_hr"),
        stddev("sensor_value").alias("stddev_hr")
    )

    # Step 2: join stats back to each reading, compute z-score
    with_stats = heart_rate_readings.join(stats, "device_id")

    flagged_zscore = with_stats.withColumn(
        "z_score",
        (col("sensor_value") - col("mean_hr")) / col("stddev_hr")
    ).withColumn(
        "is_anomaly_zscore",
        (col("z_score") > 3) | (col("z_score") < -3)   # common convention: |z| > 3 = anomaly
    ).select(col("device_id"),col("event_timestamp"),col("mean_hr"),col("stddev_hr"),col("is_anomaly_zscore"))
    return flagged_zscore

signal_sync_failure = 15
@dp.materialized_view
def signal_sync_failures():
    df = spark.read.table("silver_iot_sensor_readings")
    window_spec = Window.partitionBy("device_id").orderBy(col("event_timestamp"))
    df2 = df.withColumn("Prev_Time",lag(col("event_timestamp")).over(window_spec)).filter((col("event_timestamp")-col("Prev_Time"))>lit(signal_sync_failure))
    df3 = df2.select(col("device_id"),col("event_timestamp"),col("Prev_Time"),(minute(col("event_timestamp"))-minute(col("Prev_Time"))).alias("signal_sync_failure"))
    return df3

# @dp.materialized_view
# def Geo_distribution():
#     df = spark.read.table("silver_iot_sensor_readings")
#     df1 = df.withColumn("h3_index",expr("h3_pointtocell(lat,lon,8)")).groupBy(col("device_id"),col("h3_index")).agg(col("h3_index").alias("h3_index"))
#     return df1




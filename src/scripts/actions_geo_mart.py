import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
import findspark

findspark.init()
findspark.find()

events_path = sys.argv[1]
cities_path = sys.argv[2]
target_path = sys.argv[3]

def main() -> None:
    spark = SparkSession.builder \
                    .master("local") \
                    .appName("s7_project") \
                    .getOrCreate()
    
    get_geo_actions(event_with_corr_city(event_with_cities(events_path, cities_path, spark))).write.parquet(target_path)

def event_with_cities(events_path: str, cities_path: str, spark: SparkSession) -> DataFrame:
    events = spark.read.parquet(events_path) \
                .sample(0.05) \
                .withColumnRenamed("lat", "ev_lat") \
                .withColumnRenamed("lon", "ev_lng")
 
    cities = spark.read.csv(cities_path, sep = ";", header = True )
 
    event_with_cities = events \
                        .crossJoin(cities) \
                        .withColumn("distance", F.acos(F.sin(F.radians(F.col('msg_lat'))) * F.sin(F.radians(F.col('lat'))) + 
                                                    F.cos(F.radians(F.col('msg_lat'))) * F.cos(F.radians(F.col('lat'))) * 
                                                    F.cos(F.radians(F.col('msg_lng')) - F.radians(F.col('lng')))) * 6371)
    
    
    return event_with_cities

def event_with_corr_city(event_with_cities: DataFrame) -> DataFrame:
    window = Window().partitionBy(F.col("event.message_id")).orderBy(F.col("distance"))

    df_city = event_with_cities \
                .withColumn("rank", F.row_number().over(window)) \
                .where("rank = 1") \
                .drop("rank")

    return df_city

def get_geo_actions(corr_city: DataFrame) -> DataFrame:
    monthly_reg_cnt = corr_city \
                        .withColumn("rank", F.row_number().over(Window().partitionBy("event.message_from").orderBy("date"))) \
                        .where("rank = 1") \
                        .drop("rank") \
                        .groupBy(F.trunc(F.col("date"), "month").alias("month"), "city") \
                        .agg(F.count("*").alias("month_user")) \
                        .select("month", "city", "month_user")

    weekly_reg_cnt = corr_city \
                        .withColumn("rank", F.row_number().over(Window().partitionBy("event.message_from").orderBy("date"))) \
                        .where("rank = 1") \
                        .drop("rank") \
                        .groupBy(F.trunc(F.col("date"), "week").alias("week"), "city") \
                        .agg(F.count("*").alias("week_user")) \
                        .select("week", "city", "week_user")
    
    weekly_window = Window().partitionBy([F.trunc(F.col("date"), "week"), "city"])
    monthly_window = Window().partitionBy([F.trunc(F.col("date"), "month"), "city"])
    rank_window = Window().partitionBy(["month", "week"]).orderBy("week")

    return corr_city \
            .withColumn("month", F.trunc(F.col("date"), "month")) \
            .withColumn("week", F.trunc(F.col("date"), "week")) \
            .withColumn("week_message", F.count(F.when(F.col("event_type") == "message", "event.message_id")).over(weekly_window)) \
            .withColumn("week_reaction", F.count(F.when(F.col("event_type") == "reaction", "event.message_id")).over(weekly_window)) \
            .withColumn("week_subscription", F.count(F.when(F.col("event_type") == "subscription", "event.message_id")).over(weekly_window)) \
            .withColumn("month_message", F.count(F.when(F.col("event_type") == "message", "event.message_id")).over(monthly_window)) \
            .withColumn("month_reaction", F.count(F.when(F.col("event_type") == "reaction", "event.message_id")).over(monthly_window)) \
            .withColumn("month_subscription", F.count(F.when(F.col("event_type") == "subscription", "event.message_id")).over(monthly_window)) \
            .withColumn("rank", F.row_number().over(rank_window)) \
            .where("rank == 1") \
            .drop("rank") \
            .join(weekly_reg_cnt, ["week", "city"], "left") \
            .join(monthly_reg_cnt, ["month", "city"], "left") \
            .select("month", "week", "city", "week_message", "week_reaction", "week_subscription", "week_user", "month_message", "month_reaction", "month_subscription", "month_user") \
            .fillna(0)
            
            
if __name__ == "__main__":
    main()
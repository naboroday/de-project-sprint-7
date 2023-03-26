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

def main():
    spark = SparkSession.builder \
                    .master("local") \
                    .appName("s7_project") \
                    .getOrCreate()

    friends_recommendations(event_with_corr_city(event_with_city(events_path, cities_path, spark))).write.parquet(target_path)

def event_with_city(path_event_prqt: str, path_city_data: str, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    events_geo = spark.read.parquet(path_event_prqt) \
                    .sample(0.05) \
                    .drop('city','id') \
                    .withColumn('event_id', F.monotonically_increasing_id()) \
                    .withColumnRenamed("lat", "ev_lat") \
                    .withColumnRenamed("lon", "ev_lng") \
    
    city = spark.read.csv(path_city_data, sep=";", header=True)
 
    events_city = events_geo \
                .crossJoin(city) \
                .withColumn('distance', 2 * 6371.0 * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col("ev_lat")) - F.radians(F.col("lat")))/F.lit(2)), 2)) + \
                      F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("ev_lat"))) * \
                      F.pow(F.sin((F.radians(F.col("ev_lng")) - F.radians(F.col("lng")))/F.lit(2)), 2)))

    return events_city


def event_with_corr_city(event_with_cities: DataFrame) -> DataFrame:
    window = Window().partitionBy(F.col("event.message_id")).orderBy(F.col("distance"))

    df_city = event_with_cities \
                .withColumn("rank", F.row_number().over(window)) \
                .where("rank = 1") \
                .drop("rank")

    return df_city

def friends_recommendations(df_city: DataFrame):
    window = Window().partitionBy("event.message_from").orderBy("date")

    df_left = df_city \
                .withColumn("zone_id", F.last("city").over(window)) \
                .withColumn("local_time", F.from_utc_timestamp(
                                            F.last("event.datetime").over(window).cast("Timestamp"), 
                                            F.col("timezone"))) \
                .selectExpr("event.message_from as user_left", "event.subscription_channel", "zone_id", "local_time") \
                .distinct()
    
    df_right = df_city \
                .withColumn("zone_id", F.last("city").over(window)) \
                .withColumn("local_time", F.from_utc_timestamp(
                                            F.last("event.datetime").over(window).cast("Timestamp"), 
                                            F.col("timezone"))) \
                .selectExpr("event.message_from as user_right", "event.subscription_channel", "zone_id", "local_time") \
                .distinct()

    candidates = df_left.join(df_right, [df_left.subscription_channel == df_right.subscription_channel]) \
                .select("user_left", "user_right", df_left.zone_id, df_left.local_time) \
                .distinct() \
                .where("user_left < user_right")
    
    d_participants1 = df_city.where("event_type = 'message'").selectExpr("event.message_from as user_left", "event.message_to as user_right")
    d_participants2 = df_city.where("event_type = 'message'").selectExpr("event.message_to as user_left", "event.message_from as user_right")
    
    all_participants = d_participants1.union(d_participants2)

    join_conditions = [
        candidates.user_left == all_participants.user_left, 
        candidates.user_right == all_participants.user_right
    ]

    strangers = candidates.join(all_participants, join_conditions, "leftanti")

    df_city1 = df_city
    df_city2 = df_city

    return strangers.join(df_city1, strangers.user_left == df_city1.event.message_from) \
            .withColumnRenamed("lat", "user_left_lat") \
            .withColumnRenamed("lng", "user_left_lng") \
            .select("user_left", "user_right", "user_left_lat", "user_left_lng", "zone_id", "local_time") \
            .join(df_city2, strangers.user_right == df_city2.event.message_from) \
            .withColumnRenamed("lat", "user_right_lat") \
            .withColumnRenamed("lng", "user_right_lng") \
            .withColumn("distance", F.acos(F.sin(F.col("user_left_lat")) * F.sin(F.col("user_right_lat")) + 
                                    F.cos(F.col("user_left_lat")) * F.cos(F.col("user_right_lat")) * F.cos(F.col("user_left_lng") - F.col("user_right_lng"))) * F.lit(6371)) \
            .where("distance <= 1") \
            .withColumn("processed_dttm", F.current_date()) \
            .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")

    
if __name__ == "__main__":
    main()

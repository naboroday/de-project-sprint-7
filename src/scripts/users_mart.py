import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
import findspark
from pyspark.sql.types import DateType

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
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    cities = spark.read.csv(cities_path, sep = ";", header = True ) \
                .withColumn("lat", F.regexp_replace('lat',r'[,]',".")) \
                .withColumn("lng", F.regexp_replace('lat',r'[,]',"."))

    corr_city = event_with_corr_city(event_with_cities(events_path, cities, spark))
    
    users_mart = actial_geo(corr_city) \
                    .join(home_geo(travel_geo(corr_city)), ["user_id"]) \
                    .join(cities, ["id"]) \
                    .withColumnRenamed("city", "home_city") \
                    .join(get_travel_count(visited_cities_history(corr_city)), ["user_id"]) \
                    .join(get_travel_array(visited_cities_history(corr_city)), ["user_id"]) \
                    .join(get_local_time(actial_geo(corr_city)), ["user_id"]) \
                    .select("user_id", "act_city", "home_city", "travel_count", "travel_array", "local_time")
    
    users_mart.write.parquet(target_path)

    spark.stop()

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

def event_with_corr_city(event_with_city: DataFrame) -> DataFrame:
    window = Window().partitionBy('event.message_from').orderBy(F.col("distance"))

    df_city = event_with_city \
                .withColumn("rank", F.row_number().over(window)) \
                .where("rank = 1") \
                .drop("rank")

    return df_city

# act_city
def actial_geo(df_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
 
    window = Window().partitionBy('event.message_from').orderBy(F.col('date').desc())
 
    df_actual = df_city \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .selectExpr('event.message_from as user', 'city' , 'id as city_id') \
        .persist()
 
    return df_actual

def travel_geo(df_city: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
 
    window = Window().partitionBy('event.message_from', 'id').orderBy(F.col('date'))
 
    df_travel = df_city \
        .withColumn("dense_rank", F.dense_rank().over(window)) \
        .withColumn("date_diff", F.datediff(F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), 'dd'))) \
        .selectExpr('date_diff', 'event.message_from as user', 'date', "id" ) \
        .groupBy("user", "date_diff", "id") \
        .agg(F.countDistinct(F.col('date')).alias('cnt_city'))
 
    return df_travel

# home_city
def home_geo(df_travel: pyspark.sql.DataFrame, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
 
    df_home = df_travel \
    .withColumn('max_dt', F.max(F.col('date_diff')) \
                .over(Window().partitionBy('user')))\
    .filter((F.col('cnt_city')>27) & (F.col('date_diff') == F.col('max_dt'))) \
    .persist()
 
    return df_home

def visited_cities_history(event_with_corr_city: DataFrame) -> DataFrame:
    window = Window().partitionBy("user_id").orderBy("date")

    travelDF = event_with_corr_city \
                .withColumn("prev_city", F.lag("city").over(window)) \
                .withColumn("new_visit", F.expr("CASE WHEN city = prev_city THEN False ELSE True END")) \
                .drop("prev_city") \
                .where("new_visit = True") \
                .select("user_id", "city")
    
    return travelDF

# travel_count
def get_travel_count(visited_cities_history: DataFrame) -> DataFrame:
    return visited_cities_history \
            .groupBy("user_id") \
            .agg(F.count("city").alias("travel_count"))

# travel_array
def get_travel_array(visited_cities_history: DataFrame) -> DataFrame:
    return visited_cities_history \
            .groupBy("user_id") \
            .agg(F.collect_list("city").alias("travel_array"))

# local_time
def get_local_time(event_act_city: DataFrame) -> DataFrame:
    return event_act_city \
            .withColumn("TIME", F.col("datetime").cast("Timestamp"))\
            .withColumn("local_time", F.from_utc_timestamp(F.col("TIME"), F.col("timezone")))\
            .select("user_id", "local_time")

if __name__ == "__main__":
    main()
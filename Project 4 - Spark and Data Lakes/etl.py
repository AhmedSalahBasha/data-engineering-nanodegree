import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Function to create the Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to read, process and write back the song data.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,'song-data','*','*','*')   
    
    songs_schema = StructType([
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('artist_id', StringType()),
        StructField('year', LongType()),
        StructField('duration', DoubleType())
     ])

    # extract columns to create songs table
    songs_table = spark.read.json(song_data, schema=songs_schema)
    
    # write songs table to parquet files partitioned by year and artist
    try:
        songs_table.write \
                    .mode("overwrite") \
                    .partitionBy("year","artist_id") \
                    .parquet(output_data + "output/songs/")
        print("Songs table has been exported to partitioned parquet files successfully!")
    except Exception as err:
        print("Error {0} has occurred!".format(err))

    # extract columns to create artists table
    artist_schema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_name', StringType()),
        StructField('artist_location', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType())
     ])

    artists_table = spark.read.json(song_data, schema=artist_schema)
    
    # write artists table to parquet files
    try:
        artists_table.write.parquet(output_data + "output/artists/artists.parquet")
        print("Artists table has been exported to a parquet file successfully!")
    except Exception as err:
        print("Error {0} has occurred!".format(err))


def process_log_data(spark, input_data, output_data):
    """
    Function to read, process and write back the log data.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,'log-data','*','*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = log_df.filter(log_df.page == "NextSong")

    # extract columns for users table    
    users_table = log_df.select(["userId", "firstName", "lastName", "gender", "level"])
    
    # write users table to parquet files
    try:
        users_table.write.parquet(output_data + "output/users/users.parquet")
    except Exception as err:
        print("Error {0} has occurred!".format(err))

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", F.to_timestamp(F.col("ts") / 1000))
    
    # create time_table columns from timestamp column
    df = df.withColumn('hour', hour(log_df.timestamp))
    df = df.withColumn('day', dayofmonth(log_df.timestamp))
    df = df.withColumn('week', weekofyear(log_df.timestamp))
    df = df.withColumn('month', month(log_df.timestamp))
    df = df.withColumn('year', year(log_df.timestamp))
    df = df.withColumn('weekday', dayofweek(log_df.timestamp)) 
    
    # extract columns to create time table
    time_table = df.select(["timestamp", "hour", "day", "week", "month", "year", "weekday"])
    
    # write time table to parquet files partitioned by year and month
    try:
        time_table.write \
                .mode("overwrite") \
                .partitionBy("year","month") \
                .parquet(output_data + "output/time/")
    except Exception as err:
        print("Error {0} has occurred!".format(err))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data,'song-data','*','*','*') 
    song_df = spark.read.json(song_data)
    
    # create tempView for both dataframes
    song_df.createOrReplaceTempView("songs_data")
    df.createOrReplaceTempView("logs_data")
    
    # query for generating song_plays table by joining two dataframes
    song_plays_query = """
        SELECT
            ld.timestamp AS start_time,
            ld.year,
            ld.month,
            ld.userId AS user_id,
            ld.level,
            sd.song_id,
            sd.artist_id,
            ld.sessionId AS session_id,
            ld.location,
            ld.userAgent As user_agent
        FROM logs_data ld
        JOIN songs_data sd ON (sd.artist_name = ld.artist)
        WHERE ld.userId IS NOT NULL
    """
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(song_plays_query)
    
    # generate unique ID for songplays_table
    songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    try:
        songs_plays_table.write \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .parquet(output_data + "output/songplays/")
    except Exception as err:
        print("Error {0} has occurred!".format(err))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-project-emr/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()

if __name__ == "__main__":
    main()

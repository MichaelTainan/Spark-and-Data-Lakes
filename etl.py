import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


"""
Extract data then write to songs_table(S3 songs folder) and write to artists_table(S3 artists folder)
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(os.path.join(output_data, "songs"))
    print("Finish write parquet to songs_table.", datetime.now())
    
    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "artist_name as name", "artist_location as location", \
                              "artist_latitude as lattitude", "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, "artists"))
    print("Finish write parquet to artists_table.", datetime.now())
    
"""
Extract data then write to songs_table(S3 songs folder) and write to artists_table(S3 artists folder)
"""
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page =="NextSong")
    
    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level")\
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, "users"))
    print("Finish write parquet to users_table.", datetime.now())
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts : datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr("ts as start_time", "hour(timestamp) as hour", "dayofmonth(datetime) as day",\
                               "weekofyear(datetime) as week", "month(datetime) as month", "year(datetime) as year",\
                               "dayofweek(datetime) as weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(os.path.join(output_data, "time"))
    print("Finish write parquet to time_table.", datetime.now())
        
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/*/*.json")
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    
    song_stage = song_df.select("title", "song_id", "artist_id", "artist_name").dropDuplicates()
    song_stage.createOrReplaceTempView("songStage")
    df.createOrReplaceTempView("logStage")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT b.ts as start_time, b.userId as user_id, b.level, a.song_id, a.artist_id, b.sessionId as session_id, b.location, 
           b.userAgent as user_agent, month(b.datetime) as month, year(b.datetime) as year 
    FROM songStage a JOIN logStage b 
    ON a.title = b.song
    AND a.artist_name = b.artist
    AND b.page = 'NextSong'
    AND a.song_id IS NOT NULL
    AND a.artist_id IS NOT NULL
    """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(os.path.join(output_data, "songplays"))
    print("Finish write parquet to songplays_table.", datetime.now())

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-860223054232-us-east-1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

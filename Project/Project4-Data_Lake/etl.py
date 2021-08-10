from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#import configparser
# config = configparser.ConfigParser()
# config.read_file(open('dl.cfg'))
# os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Description:
        Creates a new Spark session with the specified configuration
    Arguments:
        None
    Return:
        The newly created spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process song data and write songs and artists table in S3
    Arguments:
        spark: spark session object 
        input_data: path in which input json data were located
        output_data: path in which output parquet data were located
    Return:
        None
    """
    print("Process song data")

    print("     Get filepath to song data file")
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    
    print("     Read song data file")
    df = spark.read.json(song_data)

    print("     Create a temporary view")
    df.createOrReplaceTempView("song")

    print("     Extract columns to create songs table: song_id, title, artist_id, year, duration")
    songs_table = spark.sql("""SELECT ROW_NUMBER() OVER(ORDER BY title) AS song_id, 
                                      title, 
                                      artist_id, 
                                      year, 
                                      duration 
                               FROM song 
                               GROUP BY title, artist_id, year, duration""")
    
    print("     Write songs table to parquet files partitioned by year and artist") 
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet("{}songs_table/".format(output_data))

    print("     Extract columns to create artists table: artist_id, name, location, latitude, longitude") 
    artists_table = spark.sql("""SELECT DISTINCT artist_id, 
                                                  artist_name      AS name,
                                                  artist_location  AS location, 
                                                  artist_latitude  AS latitude, 
                                                  artist_longitude AS longitude
                                  FROM song""")
    
    print("     Write artists table to parquet files")
    artists_table.write.mode("overwrite").parquet("{}artists_table/".format(output_data))
                                    

def process_log_data(spark, input_data, output_data):
    """
    Description:
        Process log data and write users, time, and songplays table in S3
    Arguments:
        spark: spark session object 
        input_data: path in which input json data were located
        output_data: path in which output parquet data were located
    Return:
        None
    """
    print("Process log data")

    print("     Get filepath to log data file")
    log_data = "{}log_data/*/*/*.json".format(input_data)

    print("     Read log data file")
    df = spark.read.json(log_data) 
    
    print("     Filter by actions for song plays")
    df = df.filter(df.page == 'NextSong')

    print("     Create a temporary view")
    df.createOrReplaceTempView("log")

    print("     Extract columns for users table: user_id, first_name, last_name, gender, level")
    users_table = spark.sql("""
                            SELECT DISTINCT userId AS user_id,
                                   firstName AS first_name,
                                   lastName AS last_name,
                                   gender,
                                   LAST_VALUE(level) OVER (PARTITION BY userId ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS level
                            FROM log
                            """)
    
    print("     Write users table to parquet files")
    users_table.write.mode("overwrite").parquet("{}users_table/".format(output_data))

    print("     Create timestamp column from original timestamp column")
    get_timestamp = lambda x : (col(x)/1000).cast("timestamp")
    df = df.withColumn("startTime", get_timestamp("ts"))

    print("     Create a temporary view")
    df.createOrReplaceTempView("log")
    
    print("     Extract columns to create time table")
    time_table = spark.sql("""SELECT DISTINCT startTime              AS start_time,
                                              HOUR(startTime)        AS hour,
                                              DAY(startTime)         AS day,
                                              WEEKOFYEAR(startTime)  AS week, 
                                              MONTH(startTime)       AS month,
                                              YEAR(startTime)        AS year,
                                              DAYOFWEEK(startTime)   AS weekday
                              FROM log
                           """)
    
    print("     Write time table to parquet files partitioned by year and month")
    time_table.write.partitionBy("year","month").parquet("{}time_table/".format(output_data))

    print("     Read in song data to use for songplays table: song_id, timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent")
    songs_table = spark.read.option("basePath", "{}songs_table/".format(output_data))\
                            .parquet("{}songs_table/*/*/*.parquet".format(output_data))
    artists_table = spark.read.parquet("{}artists_table/*.parquet".format(output_data))
    
    print("      Create a temporary view")
    songs_table.createOrReplaceTempView("songs")
    artists_table.createOrReplaceTempView("artists")

    print("     Extract columns from joined song and log datasets to create songplays table")
    songplays_table = spark.sql("""SELECT  ROW_NUMBER() OVER(ORDER BY startTime) AS songplay_id,
                                           startTime        AS start_time,
                                           userId           AS user_id,
                                           level            AS level,
                                           song_id          AS song_id,
                                           artist_id        AS artist_id,
                                           sessionId        AS session_id,
                                           location         AS location,
                                           userAgent        AS user_agent, 
                                           YEAR(startTime)  AS year,
                                           MONTH(startTime) AS month
                                    FROM log AS l
                                    LEFT JOIN (SELECT a.name, a.artist_id, s.song_id, s.title
                                               FROM songs AS s
                                               JOIN artists AS a ON (s.artist_id = a.artist_id)
                                               ) AS j
                                    ON (l.song = j.title AND l.artist = j.name)
                                """)

    print("     Write songplays table to parquet files")
    songplays_table.write.partitionBy("year","month").parquet("{}songplays_table/".format(output_data))

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()

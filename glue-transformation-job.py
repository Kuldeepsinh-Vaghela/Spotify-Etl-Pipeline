import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


s3_path = "s3://spotify-etl-project-kuldeep/raw-data/to-processed/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options={"paths": [s3_path]},
    format= "json"
)

spotify_df = source_dyf.toDF()

def process_albums(df):
    df_albums = df.withColumn("tracks", explode("tracks.album")).select(col("tracks.id").alias("album_id")\
                                                                            ,col("tracks.name").alias("album_name"),\
                                                                            col("tracks.release_date").alias("release_date"),col("tracks.total_tracks").alias("num_of_tracks"),\
                                                                            col("tracks.external_urls.spotify").alias("album_url")).drop_duplicates(['album_id'])
    df_albums = df_albums.withColumn("release_date", to_date("release_date"))
    return df_albums

def process_tracks(df):
    df_tracks = df.withColumn("tracks", explode("tracks")).select(col("tracks.id").alias("track_id"),\
                                                                      col("tracks.name").alias("track_name"),\
                                                          col("tracks.album.name").alias("album_name"),\
                                                                      col("tracks.album.id").alias("album_id"),\
                                                                      col("tracks.duration_ms").alias("track_duraction_ms"),\
                                                         col("tracks.explicit").alias("is_explicit"),\
                                                                      col("tracks.popularity").alias("popularity"),\
                                                          col("tracks.external_urls.spotify").alias("track_url"))
    return df_tracks
    
    
    
album_df = process_albums(spotify_df)
tracks_df = process_tracks(spotify_df)

def write_to_s3(df, path_suffix, format_type):
    dynamic_frame  = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": f"s3://spotify-etl-project-kuldeep/transformed-data/{path_suffix}/"},
        format = format_type
    )

write_to_s3(album_df, "album-data/album_data_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
write_to_s3(tracks_df, "tracks-data/tracks_data_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
job.commit()
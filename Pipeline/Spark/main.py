import os, datetime
import time
from elasticsearch import Elasticsearch
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum, lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark import SparkContext
import json
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import sys
# sys.path.append('../../src/')  # Add the parent directory to the Python path
# print(sys.path)
from model import sid
from API_Connection.Spotify import update_top_songs
from API_Connection.API_keys import spotify_client_secret

def analyze(spark, json_path):
  f = open(json_path)
  data = json.load(f)

  schema = StructType([
    StructField('1962', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('1968', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('1969', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('1970', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('1990', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2000', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2002', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2008', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2009', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2015', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2016', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2018', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2019', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2020', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2021', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True),
    StructField('2022', ArrayType(StructType([
        StructField('artist', StringType(), True),
        StructField('lyrics', StringType(), True),
        StructField('title', StringType(), True)
    ]), True), True)
  ])

  df = spark.read.json(spark.sparkContext.parallelize([data]), multiLine=True, schema=schema)

  df.show()

  rows = df.collect()

  data_to_add = []

  labels = df.columns

  for i, year in enumerate(rows[0]):
    for entry in year:
      sentiment = sid.polarity_scores(entry.lyrics)
      data_to_add.append([labels[i], entry.title, entry.lyrics, sentiment["neg"], sentiment["neu"], sentiment["pos"], sentiment["compound"]])

  columns = ["year", "title", "lyrics", "sentiment_neg", "sentiment_neu", "sentiment_pos", "sentiment_compound"]
  output_df = spark.createDataFrame(data_to_add, columns)
  output_df.show()

  return output_df


if __name__ == "__main__": 
  spark = SparkSession.builder \
      .appName("LyricsLens")\
      .master("local[8]")\
      .config("spark.driver.memory","16G")\
      .config("spark.driver.maxResultSize", "0") \
      .config("es.nodes", "http://localhost:9200") \
      .config("es.port", "9200") \
      .getOrCreate()

  old_songs = analyze(spark, json_path='songs/song_data.json')
  old_songs.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "my_index") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .save()
  
  if spotify_client_secret != 'YOUR CLIENT SECRET':
        update_top_songs()
        new_songs = analyze(spark, json_path='songs/top_50.json')
        old_songs.write.format("org.elasticsearch.spark.sql") \
            .option("es.resource", "my_index") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .save() 
else:
    print("You need to configure your Spotify API Key")
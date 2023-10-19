import os, datetime
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, col, sum, lit
from pyspark.sql import SparkSession
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import col, explode
from pyspark import SparkContext
import json

def load_songs(spark, json_path):
  # Opening JSON file
  f = open(json_path)
  
  # returns JSON object as a dictionary
  data = json.load(f)
  
  df = spark.read.json(spark.sparkContext.parallelize([data]))

  # Iterate over the rows
  rows = df.collect()

  for row in rows:
      print(row[3])
      #print(row[1][1].artist)
      print(row[1][1].lyrics)
      print(row[1][1].title)
      print(type(row[0]))

if __name__ == "__main__":
  spark = SparkSession.builder \
      .appName("LyricsLens")\
      .master("local[8]")\
      .config("spark.driver.memory","16G")\
      .config("spark.driver.maxResultSize", "0") \
      .config("spark.kryoserializer.buffer.max", "2000M")\
      .getOrCreate()

  load_songs(spark, json_path='songs/song_data.json')
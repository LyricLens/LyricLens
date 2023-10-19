import os, datetime
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

  # output_schema = StructType([
  #   StructField("year", StringType(), True),
  #   StructField("title", StringType(), True),
  #   StructField("sentiment_neg", StringType(), True),
  #   StructField("sentiment_neu", StringType(), True),
  #   StructField("sentiment_pos", StringType(), True),
  #   StructField("sentiment_compound", StringType(), True),
  # ])

  data_to_add = []

  labels = df.columns

  for i, year in enumerate(rows[0]):
    for entry in year:
      sentiment = sid.polarity_scores(entry.lyrics)
      data_to_add.append([labels[i], entry.title, sentiment["neg"], sentiment["neu"], sentiment["pos"], sentiment["compound"]])

  columns = ["year", "title", "sentiment_neg", "sentiment_neu", "sentiment_pos", "sentiment_compound"]
  output_df = spark.createDataFrame(data_to_add, columns)
  output_df.show()

  return output_df


es_mapping_modified = {
    "mappings":{
        "properties":{
            "year":{"type":"text"},
            "title":{"type":"text"},
            "sentiment_neg":{"type":"text"},
            "sentiment_neu":{"type":"text"},
            "sentiment_pos":{"type":"text"},
            "sentiment_compound":{"type":"text"},
        }
    }
}

# elastic_host = "elasticsearch"

# elastic_topic = "tap" #TODO rename
# elastic_index = "tap"

# es = Elasticsearch(hosts=elastic_host)


# es.indices.create(
#     index=elastic_index,
#     body=es_mapping_modified,
#     ignore=400  
# )


if __name__ == "__main__": 
  # sparkConf = SparkConf().set("spark.app.name", "LyricsLens") \
  #     .set("es.nodes", elastic_host) \
  #     .set("es.port", "9200") \
  #     .set("spark.executor.heartbeatInterval", "200000") \
  #     .set("spark.network.timeout", "300000")

  # sc = SparkContext.getOrCreate(conf=sparkConf)
  # spark = SparkSession(sc)
  spark = SparkSession.builder.appName("ElasticsearchExample").getOrCreate()

  # spark = SparkSession.builder \
  #     .appName("LyricsLens")\
  #     .master("local[8]")\
  #     .config("spark.driver.memory","16G")\
  #     .config("spark.driver.maxResultSize", "0") \
  #     .config("spark.kryoserializer.buffer.max", "2000M")\
  #     .set("es.nodes", elastic_host) \
  #     .set("es.port", "9200") \
  #     .getOrCreate()

  sentiment = analyze(spark, json_path='songs/song_data.json')
  sentiment.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .save("your_elasticsearch_type")
  # sentiment \
  #   .write \
  #   .option("checkpointLocation", "/tmp/checkpoints") \
  #   .format("org.elasticsearch.spark.sql") \
  #   .start(elastic_index) \
  #   .awaitTermination()
    # .format("es") \
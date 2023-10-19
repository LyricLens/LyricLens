FROM jupyter/pyspark-notebook:spark-3.1.1

RUN pip3 install pyspark kafka-utils elasticsearch matplotlib requests Pillow nltk elasticsearch
#TODO vediamo quali di questi moduli serve e quali no e li rimuoviamo da qua, mettendoli in un requirements.txt
WORKDIR /app
ADD main.py .
ADD model.py .
COPY song_data.json songs/song_data.json

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1", "--master", "local[*]", "main.py"]
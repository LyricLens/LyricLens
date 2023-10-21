FROM jupyter/pyspark-notebook:spark-3.1.1

WORKDIR /app
ADD main.py .
ADD model.py .
ADD API_keys.py .
ADD Spotify.py .
ADD requirements.txt .

RUN pip3 install -r requirements.txt
RUN pip3 install nltk 

COPY song_data.json songs/song_data.json

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1", "--master", "local[*]", "main.py"]
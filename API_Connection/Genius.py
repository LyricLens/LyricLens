from lyricsgenius import Genius
import API_keys
import csv
import json
import re

client_access_token = API_keys.genius_client_access_token
genius = Genius(client_access_token)

# Load the CSV with the songs
file = open('songs/song_data.json')
csvreader = csv.reader(file)

# Dictionary to store data by year
song_data = {}

with open("song_data.json", "w") as outfile:
    for row in csvreader:
        try:
            year = row[0]
            title = row[1]
            artist = row[2]

            # Get the song lyrics
            song = genius.search_song(title, artist)

            if song is None or song.lyrics == "":
                continue

            # Lyrics saving and splitting, avoinding "\n" keyword
            lyrics = str(song.lyrics.split("\n",1)[1:])

            if lyrics[0] == "[":
                lyrics = lyrics[1:]
            
            lyrics = re.sub("[\[].*?[\]]", "", lyrics)

            # Create a new song dictionary
            new_song = {
                "title": title,
                "artist": artist,
                "lyrics": lyrics
            }

            # Check if the year key exists in the dictionary, if not, create it
            if year not in song_data:
                song_data[year] = []

            # Append the new song to the list of songs for the respective year
            song_data[year].append(new_song)

        except Exception as e:
            print(e)

    # Writing to song_data.json
    json.dump(song_data, outfile, indent=4)
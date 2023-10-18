import spotipy
import API_keys
import json
import re
from spotipy.oauth2 import SpotifyOAuth
from lyricsgenius import Genius

# Initialize the Spotify API client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=API_keys.spotify_client_id,
                                               client_secret=API_keys.genius_client_access_token,
                                               redirect_uri='http://127.0.0.1:8888/callback',
                                               scope='user-top-read'))

client_access_token = API_keys.genius_client_access_token
genius = Genius(client_access_token)

csv_file = "top_50songs.csv"
# Function to get the top 50 songs and update the JSON file
def update_top_songs():
    top_tracks = sp.playlist_items('https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF?si=f63b2ffa40454c85')

    data = []
    song_data = {}

    # Update the JSON file
    data.append([(track['track']['name'], track['track']['artists'][0]['name']) for track in top_tracks['items']])
    with open("top50_2023.json", "w") as outfile:
        for item in data[0]:
            try:
                year = 2023
                title = item[0]
                artist = item[1]

                # Get the song lyrics
                song = genius.search_song(title, artist)

                if song is None or song.lyrics == "":
                    continue

                # Lyrics saving and splitting, avoinding "\n" keyword
                lyrics = str(song.lyrics.split("\n", 1)[1:])

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

update_top_songs()
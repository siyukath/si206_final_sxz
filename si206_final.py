import json
import unittest
import os
import requests
import sqlite3
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import numpy as np
import csv
from bs4 import BeautifulSoup

#Spotify unique request
#reference: https://developer.spotify.com/documentation/web-api/reference/#/
CLIENT_ID = '83838e72997941ce974e44c7cc023032'
CLIENT_SECRET = '1261d1f31b524061bfc0b50638ddabf3'
AUTH_URL = 'https://accounts.spotify.com/api/token'
BASE_URL = 'https://api.spotify.com/v1/'

#Youtube api request link
#reference: https://blog.hubspot.com/website/how-to-get-youtube-api-key
def get_youtube_url(id):
    Youtube_API_KEY = "AIzaSyD8UwSNLPju2h42Qxh654RQ2jo494qlAKw"
    url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={id}&key={Youtube_API_KEY}"
    return url

#Itune api instruction
#reference: https://performance-partners.apple.com/search-api
base_url = 'https://itunes.apple.com/search?'




# access Spotify artist data based unique id
def get_spotify_data(list):
    spotify_data = []
    auth_response = requests.post(AUTH_URL, {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    })

    if auth_response.status_code != 200:
        print(f"Failed to authenticate with Spotify API. Status code: {auth_response.status_code}")
        print(f"Response: {auth_response.text}")
        return spotify_data

    # convert the json response
    auth_response_data = auth_response.json()
    # save the access token
    access_token = auth_response_data['access_token']

    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}
    # Track ID from the URI
    for i in range(0,len(list)):
        track_id = list[i]
        # actual GET request with proper header
        r = requests.get(BASE_URL + 'artists/' + track_id, headers=headers)
        #print(r)

        if r.status_code != 200:
            print(f"Failed to fetch data for track ID {track_id}. Status code: {r.status_code}")
            print(f"Response: {r.text}")
            continue  # Skip this iteration and continue with the next track ID


        d = r.json()
        spotify_data.append(d)
        #print(d)
    return spotify_data

# access Youtube channel data with specific id
def get_youtube_data(list):
    youtube_data = []
    for i in range(0,len(list)):
        url = get_youtube_url(list[i])
        response = requests.get(url)
        d = response.json()
        youtube_data.append(d)
    # print(youtube_data)
    return youtube_data

# access itune data with artist name
def get_itune_data(artist_list):
    itune_data = []
    for artist in artist_list:
        url = f'https://itunes.apple.com/search?term={artist}&entity=musicTrack&limit=1'
        response = requests.get(url)
        if response.status_code == 200:
            try:
                data = response.json()
                results = data.get('results', [])
                if results:
                    # Extract the required fields
                    artist_name = results[0].get('artistName', 'N/A')
                    genre = results[0].get('primaryGenreName', 'N/A')
                    track_time = results[0].get('trackTimeMillis', 'N/A')
                    itune_data.append((artist_name, genre, track_time))
            except ValueError as e:
                print(f"Error decoding JSON for {artist}: {e}")
        else:
            print(f"Failed to fetch data for {artist}. Status code: {response.status_code}")

    return itune_data




# set up the database tables
def open_database(db_name):
    dir = os.path.dirname(__file__) + os.sep
    conn = sqlite3.connect(dir+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def add_name_data(artist_list, youtube_id_list, spotify_id_list, itune_id_list, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS names (id INTEGER PRIMARY KEY, name TEXT UNIQUE, youtube_id STRING, spotify_id STRING, itune_id STRING)")
    
    # Check if the itune_id column exists
    cur.execute("PRAGMA table_info(names)")
    columns = cur.fetchall()
    column_names = [col[1] for col in columns]
    
    if 'itune_id' not in column_names:
        # If itune_id column doesn't exist, add it to the table
        cur.execute("ALTER TABLE names ADD COLUMN itune_id STRING")
    
    try:
        cur.execute('SELECT id FROM names WHERE id = (SELECT MAX(id) FROM names)')
        start = cur.fetchone()
        start = start[0]
    except:
        start = 0
    
    id = 0
    for item in artist_list[start:start+25]:
        item_id = id + start
        cur.execute("INSERT OR IGNORE INTO names (id, name, youtube_id, spotify_id, itune_id) VALUES (?,?,?,?,?)",(item_id, item, youtube_id_list[item_id], spotify_id_list[item_id], itune_id_list[item_id]))
        id += 1
    
    conn.commit()


#add Itune data
def add_itune_data(itune_data, cur, conn):
    # Create the iTunes table if it doesn't exist
    cur.execute('''CREATE TABLE IF NOT EXISTS itune 
                   (id INTEGER PRIMARY KEY, artistName TEXT, primary_genre_name TEXT, trackTimeMillis INTEGER)''')

    try:
        # Retrieve the highest ID currently in the itune table
        cur.execute('SELECT MAX(id) FROM itune')
        start = cur.fetchone()[0]
        start = start + 1 if start is not None else 0
    except Exception as e:
        print(f"Error fetching max ID from itune table: {e}")
        start = 0

    # Insert data into the itune table
    for i, item in enumerate(itune_data[start:start+25], start=start):
        artistName, genre, trackTimeMillis = item
        cur.execute('''INSERT INTO itune (id, artistName, primary_genre_name, trackTimeMillis) 
                       VALUES (?, ?, ?, ?)''', (i, artistName, genre, trackTimeMillis))

    conn.commit()


def itune_rank_genres_by_popularity(cur):
    cur.execute("SELECT primary_genre_name, COUNT(*) as track_count FROM itune GROUP BY primary_genre_name ORDER BY track_count DESC")
    return cur.fetchall()

def itune_rank_ave_track_time_by_genre(cur):
    cur.execute("SELECT primary_genre_name, AVG(trackTimeMillis) as avg_track_time FROM itune GROUP BY primary_genre_name ORDER BY avg_track_time DESC")
    return cur.fetchall()




#add spotify data
def add_spotify_data(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS spotify (id INTEGER PRIMARY KEY, popularity INTEGER, followers INTEGER, genres STRING)")
    try:
        cur.execute('SELECT id FROM spotify WHERE id = (SELECT MAX(id) FROM spotify)')
        start = cur.fetchone()
        start = start[0]
    except:
        start = 0
    id = 0
    for item in data[start:start+25]:
        item_id = id + start
        popularity = item['popularity']
        followers = item['followers']['total']
        if len(item['genres']) > 0:
            genre = item['genres'][0]
        else:
            genre = 'n/a'
        cur.execute("INSERT OR IGNORE INTO spotify (id,popularity,followers,genres) VALUES (?,?,?,?)",(item_id, popularity, followers, genre))
        id += 1
    conn.commit()

    
#add youtube data
def add_youtube_data(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS youtube (id INTEGER PRIMARY KEY, viewcount INTEGER, subscribercount INTEGER, videocount INTEGER)")
    try:
        cur.execute('SELECT id FROM youtube WHERE id = (SELECT MAX(id) FROM youtube)')
        start = cur.fetchone()
        start = start[0]
    except:
        start = 0
    id = 0
    for item in data[start:start+25]:
        item_id = id + start
        viewcount = item['items'][0]['statistics']['viewCount']
        subscribercount = item['items'][0]['statistics']['subscriberCount']
        videocount = item['items'][0]['statistics']['videoCount']
        cur.execute("INSERT OR IGNORE INTO youtube (id,viewcount,subscribercount,videocount) VALUES (?,?,?,?)",(item_id, viewcount, subscribercount, videocount))
        id += 1
    conn.commit()

def youtube_total_views_rank(cur, conn):
    cur.execute("SELECT names.id, names.name, youtube.viewcount FROM youtube JOIN names ON names.id = youtube.id")
    youtube_totalview = cur.fetchall()
    youtube_totalview_rank = sorted(youtube_totalview, key=lambda x:x[2])
    # print(youtube_totalview_rank)
    return youtube_totalview_rank

def youtube_subscribers_rank(cur, conn):
    cur.execute("SELECT names.id, names.name, youtube.subscribercount FROM youtube JOIN names ON names.id = youtube.id")
    youtube_subscriber = cur.fetchall()
    # print(youtube_subscriber)
    youtube_subscriber_rank = sorted(youtube_subscriber, key=lambda x:x[2])
    # print(youtube_subscriber_rank)
    return youtube_subscriber_rank

def youtube_ave_views_rank(cur, conn):
    ave_view_rank = []
    cur.execute("SELECT names.id, names.name, youtube.viewcount, youtube.videocount FROM youtube JOIN names ON names.id = youtube.id")
    youtube_data = cur.fetchall()
    # print(youtube_data)
    for item in youtube_data:
        ave_view = item[2]/item[3]
        ave_view = round(ave_view, 2)
        # print(ave_view)
        ave_view_rank.append((item[0],item[1],ave_view))
        # print(ave_view_rank)
    youtube_ave_view_rank = sorted(ave_view_rank, key=lambda x:x[2])
    return youtube_ave_view_rank

def spotify_followers_rank(cur, conn):
    cur.execute("SELECT names.id, names.name, spotify.followers FROM spotify JOIN names ON names.id = spotify.id")
    spotify_data = cur.fetchall()
    spotify_follower_rank = sorted(spotify_data, key=lambda x:x[2])
    # print(spotify_follower_rank)
    return spotify_follower_rank

def spotify_popularity_rank(cur, conn):
    cur.execute("SELECT names.id, names.name, spotify.popularity FROM spotify JOIN names ON names.id = spotify.id")
    spotify_data = cur.fetchall()
    spotify_popularity_rank = sorted(spotify_data, key=lambda x:x[2])
    #print(spotify_popularity_rank)
    return spotify_popularity_rank

def spotify_genres_followers_rank(cur, conn):
    genres_dict = {}
    cur.execute("SELECT names.id, names.name, spotify.followers, spotify.genres FROM spotify JOIN names ON names.id = spotify.id")
    data = cur.fetchall()
    for item in data:
        genres_dict[item[3]] = genres_dict.get(item[3],item[2]) + item[2]
    genres_followers_rank = sorted(genres_dict.items(), key=lambda item:item[1])
    # print(genres_followers_rank)
    return genres_followers_rank


#graph charts
def youtube_total_views_rank_chart(data):
    name_list = [item[1] for item in data[:20]]
    view_list = [item[2] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, view_list, color='orange')
    plt.xlabel('name')
    plt.ylabel('total views')
    plt.title('US artitst total views ranking on Youtube top 20 ')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=6)
    plt.show()

def youtube_subscribers_rank_chart(data):
    name_list = [item[1] for item in data[:20]]
    subscriber_list = [item[2] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, subscriber_list, color='orange')
    plt.xlabel('name')
    plt.ylabel('number of subscribers')
    plt.title('US top 20 artitst subscriber count ranking on Youtube')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=6)
    plt.show()

def youtube_ave_views_rank_chart(data):
    name_list = [item[1] for item in data][:20]
    ave_view_list = [item[2] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, ave_view_list, color='orange')
    plt.xlabel('name')
    plt.ylabel('average views')
    plt.title('US artitst video average views ranking on Youtube top 20')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=6)
    plt.show()

def spotify_followers_rank_chart(data):
    name_list = [item[1] for item in data[:20]]
    follower_list = [item[2] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, follower_list, color='blue')
    plt.xlabel('name')
    plt.ylabel('number of followers')
    plt.title('US top 20 artitst followers ranking on Spotify')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=6)
    plt.show()

def spotify_popularity_rank_chart(data):
    name_list = [item[1] for item in data[:20]]
    popularity_list = [item[2] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, popularity_list, color='blue')
    plt.xlabel('name')
    plt.ylabel('popularity')
    plt.title('US artitst popularity ranking on Spotify top 20')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=6)
    plt.show()

def spotify_genres_followers_rank_chart(data):
    name_list = [item[0] for item in data[:20]]
    genre_follower_list = [item[1] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, genre_follower_list, color='blue')
    plt.xlabel('genres')
    plt.ylabel('number of followers')
    plt.title('US top 20 music genres follower numbers ranking on Spotify')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=6)
    plt.show()

def itune_visualize_genre_ranking(genre_data):
    genres, track_counts = zip(*genre_data)

    plt.figure(figsize=(12, 6))
    plt.bar(genres, track_counts, color='green')
    plt.xlabel('Genre')
    plt.ylabel('Number of Tracks')
    plt.title('Most Popular Genres on iTunes')
    plt.xticks(rotation=45)
    plt.show()

def itune_visualize_ave_track_time_by_genre(track_data):
    genres, avg_track_times = zip(*track_data)
    plt.figure(figsize=(12, 6))
    plt.bar(genres, avg_track_times, color='green')
    plt.xlabel('Genre')
    plt.ylabel('Average Track Time (milliseconds)')
    plt.title('Average Track Time by Genre on iTunes')
    plt.xticks(rotation=45)
    plt.show()

def count_total_scores(data1, data2, data3, data4, data5):
    total_score_list = []
    total_score_dict = {}
    for a in range(len(data1)):
        score_list = []
        artist = data1[a][1]
        total_score = a + 1
        score_list.append(a+1)
        for b in range(len(data2)):
            if artist == data2[b][1]:
                total_score += b + 1
                score_list.append(b+1)
        for c in range(len(data3)):
            if artist == data2[c][1]:
                total_score += c + 1
                score_list.append(c+1)
        for d in range(len(data4)):
            if artist == data2[d][1]:
                total_score += d + 1
                score_list.append(d+1)
        for e in range(len(data5)):
            if artist == data2[e][1]:
                total_score += e + 1
                score_list.append(e+1)
        total_score_list.append((artist,total_score))
        total_score_dict[artist] = score_list
    total_score_list = sorted(total_score_list, key=lambda x:x[1])
    return total_score_list


def total_rank_chart(data):
    name_list = [item[0] for item in data[:20]]
    score_list = [item[1] for item in data[:20]]
    plt.figure(figsize=(12, 6))
    plt.bar(name_list, score_list,color='red')
    plt.xlabel('name')
    plt.ylabel('total popularity score')
    plt.title('US artists total popularity ranking top 20')
    plt.xticks(name_list, rotation=90)
    plt.tick_params(axis='x', labelsize=7)
    plt.show()

def write_csv_aveview(data, filename):
    #print(data)
    with open(filename, mode = "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        header =  ['rank', 'artist', 'avg views']
        writer.writerow(header)
        for tup in data:
            writer.writerow(tup)
        return writer

def write_csv_genre(data, filename):
    #print(data)
    with open(filename, mode = "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        header =  ['genre', 'number of followers']
        writer.writerow(header)
        for tup in data:
            writer.writerow(tup)
        return writer

def write_csv_song(data, filename):
    #print(data)
    with open(filename, mode = "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        header =  ['genre', 'number of followers']
        writer.writerow(header)
        for tup in data:
            writer.writerow(tup)
        return writer

def write_csv_total(data, filename):
    #print(data)
    with open(filename, mode = "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        header =  ['artist', 'total score']
        writer.writerow(header)
        for tup in data:
            writer.writerow(tup)
        #print(writer)
        return writer

def main():
    cur, conn = open_database('music.db')
    artist_list = ['The Chainsmokers', 'blackbear', 'Lil Skies', 'Lil Pump', 'Nate Smith', 'Armani White', 'Jackson Dean', 'Jelly Roll', 'Jax', 'Jordan Davis', 'Riley Green', 'Thomas Rhett', 'Lainey Wilson', 'HARDY', 'Lana Del Rey', 'Britney Spears', 'Elton John', 'Sia', 'Kane Brown', 'Brenda Lee', 'Cole Swindell', 'Tems', 'Future', 'Mariah Carey', 'Chris Brown', 'Isabel LaRosa', 'Chencho Corleone', 'Cardi B', 'GloRilla', 'Manuel Turizo', 'Selena Gomez', 'Rema', 'ThxSoMch', 'Quavo', 'Bailey Zimmerman', 'Lizzo', 'Glass Animals', 'The 1975', 'Imagine Dragons', 'Sabrina Carpenter', 'Omar Apollo', 'Joji', 'Noah Kahan', 'Nicky Youre', 'JVKE', 'Robin Schulz', 'Oliver Tree', 'Pharrell Williams', 'Stephen Sanchez', 'Latto', 'Bebe Rexha', 'David Guetta', 'OneRepublic', 'Tate McRae', 'Tiësto', 'Meghan Trainor', 'd4vd', 'Lil Nas X', 'Taylor Swift', 'Drake', 'Bad Bunny', 'Kanye West', 'The Weeknd', 'Juice WRLD', 'Lil Baby', 'Kendrick Lamar', 'Morgan Wallen', 'YoungBoy Never Broke Again', 'Harry Styles', 'Post Malone', '21 Savage', 'Doja Cat', 'J. Cole', 'Eminem', 'XXXTENTACION', 'Lil Uzi Vert', 'Polo G', 'Justin Bieber', 'Mac Miller', 'Billie Eilish', 'Travis Scott', 'Ariana Grande', 'Ed Sheeran', 'Bruno Mars', 'Jack Harlow', 'Kodak Black', 'Trippie Redd', 'Playboi Carti', 'Luke Combs', 'Rihanna', 'Beyoncé', 'Zach Bryan', 'Nicki Minaj', 'SZA', 'Olivia Rodrigo', 'Khalid', 'Yeat', 'Metro Boomin', 'Sam Smith', 'Kim Petras']
    spotify_id_list = ['69GGBxA162lTqCwzJG5jLp','2cFrymmkijnjDg9SS92EPM','7d3WFRME3vBY2cgoP38RDo','3wyVrVrFCkukjdVIdirGVY','4NYMUsIcUUsBHbV9DICa5x','2qAwMsiIjTzlmfAkXKvhVA','0VkWDV0Bfd0EkXvaKAXUTl','19k8AgwwTSxeaxkOuCQEJs','7DQYAz99eM3Y5PkP9WtUew','77kULmXAQ6vWer7IIHdGzI','2QMsj4XJ7ne2hojxt6v5eb','6x2LnllRG5uGarZMsD4iO8','6tPHARSq45lQ8BSALCfkFC','5QNm7E7RU2m64l6Gliu8Oy','00FQb4jTyendYWaN8pK0wa','26dSoYclwsYLMAKD3tpOr4','3PhoLpVuITZKcymswpck5b','5WUlDfRSoLAfcVSX1WnrxN','3oSJ7TBVCWMDMiYjXNiCKE','4cPHsZM98sKzmV26wlwD2W','1mfDfLsMxYcOOZkzBxvSVW','687cZJR45JO7jhk1LHIbgq','1RyvyyTE3xzB2ZywiAwp0i','4iHNK0tOyZPYnBU7nGAgpQ','7bXgB6jMjp9ATFy66eO08Z','5arKwJZEvT5uKq4o0JfqR4','37230BxxYs9ksS7OkZw3IU','4kYSro6naA4h99UJvo89HB','2qoQgPAilErOKCwE2Y8wOG','0tmwSHipWxN12fsoLcFU3B','0C8ZW7ezQVs4URX5aX7Kqx','46pWGuE3dSwY3bMMXGBvVS','4MvZhE1iuzttcoyepkpfdF','0VRj0yCOv2FXJNP47XQnx5','3win9vGIxFfBRag9S63wwf','56oDRnqbIiwx4mymNEv7dS','4yvcSjfu4PC0CYQyLy4wSq','3mIj9lX2MWuHmhNCA7LSCW','53XhwfbYqKCa1cC15pYq2q','74KM79TiuVKeVCqs8QtB0B','5FxD8fkQZ6KcsSYupDVoSO','3MZsBdqDrRTJihTHQrO6Dq','2RQXRUsr4IW1f3mKyKsy4B','7qmpXeNz2ojlMl2EEfkeLs','164Uj4eKjl6zTBKfJLFKKK','3t5xRXzsuZmMDkQzgOX35S','6TLwD7HPWuiOzvXEa3oCNe','2RdwBSPQiwcmiDo9kixcl8','5XKFrudbV4IiuE5WuTPRmT','3MdXrJWsbVzdn6fe5JYkSQ','64M6ah0SkkRsnPGtGiRAbb','1Cs0zKBU1kc0i8ypK3B9ai','5Pwc4xIPtQLFEnJriah9YJ','45dkTj5sMRSjrmBSBeiHym','2o5jDhtHVPhrJdv3cEQ99Z','6JL8zeS1NmiOftqZTRgdTz','5y8tKLUfMvliMe8IKamR32','7jVv8c5Fj3E9VhNjxT4snq','06HL4z0CvFAxyc27GXpf02','3TVXtAsR1Inumwj472S9r4','4q3ewBCX7sLwd24euuV69X','5K4W6rqBFWDnAN6FQUkS6x','1Xyo4u8uXC1ZmMpatF05PJ','4MCBfE4596Uoi2O4DtmEMz','5f7VJjfbwm532GiveGC0ZK','2YZyLoL8N0Wb9xBt1NhZWg','4oUHIQIBe0LHzYfvXNW4QM','7wlFDEWiM5OoIAt8RSli8b','6KImCVD70vtIoJWnq6nGn3','246dkjvS1zLTtiykXe5h60','1URnnhqYAYcrqrcwql10ft','5cj0lLjcoR7YOSnhnX0Po5','6l3HvQ5sa6mXTsMTB19rO5','7dGJo4pcD2V6oG8kP0tJRR','15UsOTVnJzReFVN1VCnxy4','4O15NlyKLIASxsJ0PrXPfz','6AgTAQt8XS6jRWi4sX7w49','1uNFoZAHBGtllmzznpCI3s','4LLpKhyESsyAXpc4laK94U','6qqNVTkY8uBg9cP3Jd7DAH','0Y5tJX1MQlPlqiwlOH1tJY','66CXWjxzNUsdJxJ2JdwvnR','6eUKZXaKkcviH0Ku9w2n3V','0du5cEVh5yTK9QJze8zA0C','2LIk90788K0zvyj2JJVwkJ','46SHBwWsqBkxI7EeeBEQG7','6Xgp2XMz1fhVYe7i6yNAax','699OTQXzgjhIYAHMy9RyPD','718COspgdWOnwOFpJHRZHS','5pKCCKE2ajJHZ9KAiaK11H','6vWDO969PvNqNYHIOW5v0m','40ZNYROS4zLfyyBSs2PGe2','0hCNtLu0JehylgoiP8L4Gh','7tYKF4w9nC0nq9CsPZTHyP','1McMsnEElThX1knmY4oliG','6LuN9FCkKOj5PcnpouEgny','3qiHUAX7zY4Qnjx8TNUzVx','0iEtIxbK0KxaSlF7G42ZOp','2wY79sveU1sp5g7SokKOiI','3Xt3RrJMFv5SZkCfUE8C1J']
    youtube_id_list =['UCq3Ci-h945sbEYXpVlw7rJg','UCtAPxCyQaMxr61zBYFCqsFQ','UCLEyFXOmaIgG6h4_6wqLx7Q','UC6pjHMC4QXMi4llCCjtDXWg','UC7UNe3j1xMFEX2xr9TG9xAg','UCY9MU4TO25x1lec9qBNqsQw','UC0rFBO0NLx8Qz0LEZ-jk3qA','UCtyzzW6rIQiGP7gfGTXkhDw','UC60kwqBXG52dCd-_oUphnwA','UCLEhUvVUBhH8Q9Mx6Bz3BQA','UCSaJ4_YK4luUvkc9lDrwfKg','UCr7r9JorcxMDOC5ZQ_ZCs3w','UCZkeQWzPCGeH707eRS6K6Hg','UCfaTFCTvP8LVmJ4vZNUIRyQ','UCqk3CdGN_j8IR9z4uBbVPSg','UCgffc95YDBlkGrBAJUHUmXQ','UCcd0tBtip8YzdTCUw3OVv_Q','UCN9HPn2fq-NL8M5_kp4RWZQ','UC2QTDn02Xobvy_N2bb_Zzlw','UCPxuu5PdddUZcSR7KonyMWw','UCot3LeHcLzvWproxs3rNZTQ','UCWfi5ELXGAe-DCA6cOP3aNw','UCSDvKdIQOwTfcyOimSi9oYA','UCurpiDXSkcUbgdMwHNZkrCg','UCcYrdFJF7hmPXRNaWdrko4w','UCFiCc03UdOpKn0ib3UcSBSQ','UCTStLchCIe1xO-y1Oi1lruQ','UCxMAbVFmxKUVGAll0WVGpFw','UC9bZ9eWvF0eXVqrxK9ve7Nw','UC5Jn-9jqrVvKm9Hx0WW8Pgw','UCPNxhDvTcytIdvwXWAm43cA','UCHGF6zfD2gwLuke95X3CKFQ','UCksiJY4ym5DeDb-5mUfkMSA','UCU_xT0uVi5cku7cg9hDgkMA','UC2I1O7O6e8BDSXatTCpYBgQ','UCXVMHu5xDH1oOfUGvaLyjGg','UCJTs-KheOMNstaGrDL4K55Q','UC_LfW1R3B0of9qOw1uI-QNQ','UCT9zcQNlyht7fRlcjmflRSA','UCPKWE1H6xhxwPlqUlKgHb_w','UCiVLSJ2MpNteP1oLYfu0VTw','UCFl7yKfcRcFmIUbKeCA-SJQ','UCXY5pi3MbsaP1WEgClmglsA','UChofQzs5eedlpnVIbAhxNJw','UCSOfUqPoqpp5aWDDnAyv94g','UCLVVBWrp9jw4-SYUoU42hcg','UCHcb3FQivl6xCRcHC2zjdkQ','UCNUbNl2U6Hg8J0Zem6hzC2g','UCkT9AFJN7QIzApwhT825V7A','UCRQ6wJbGwbF9Wmvp5HfT7Pg','UC5-gWZXAQqSGVfPHkA7NRiQ','UC1l7wYrva1qCH-wgqcHaaRg','UCi4EDAgjULwwNBHOg1aaCig','UCQh6LB206jF3JxpCDD-fp5Q','UCPk3RMMXAfLhMJPFpQhye9g','UCkXgEcpoTE4tHsebYBouWpA','UC98WsFnuhfS3uT8PwdYCjbw','UC_uMv3bNXwapHl8Dzf2p01Q','UCqECaJ8Gagnn7YCbPEzWH6g','UCByOQJjav0CUDwxCk-jVNRQ','UCmBA_wu8xGg1OfOkfW13Q0Q','UCs6eXM7s8Vl5WcECcRHc2qQ','UC0WP5P-ufpRfjbNrmOWwLBQ','UC0BletW9phE4xHFM44q4qKA','UCVS88tG_NYgxF6Udnx2815Q','UC3lBXcrKFnFAFkfVk5WuKcQ','UCzIyoPv6j1MAZpDHKLGP_eA','UClW4jraMKz6Qj69lJf-tODA','UCZFWPqqPkFlNwIxcpsLOwew','UCeLHszkByNZtPKcaVXOCOQQ','UCOjEHmBKwdS7joWpW0VrXkg','UCzpl23pGTHVYqvKsgY0A-_w','UCnc6db-y3IU7CkT_yeVXdVg','UCfM3zsQsOnfWNUppiycmBuw','UCM9r1xn6s30OnlJWb-jc3Sw','UCqwxMqUcL-XC3D9-fTP93Mg','UC0ifXd2AVf1LMYbqwB5GH4g','UCIwFjwMjI0y7PDBVEO9-bkQ','UC3SEvBYhullC-aaEmbEQflg','UCiGm_E4ZwYSHV3bcW1pnSeQ','UCtxdfwb9wfkoGocVUAJ-Bmg','UC9CoOnJkIBMdeijd9qYoT_g','UC0C-w0YjGpqDXGB8IHb662A','UCoUM-UJ7rirJYP8CQ0EIaHA','UC6vZl7Qj7JglLDmN_7Or-ZQ','UChEYVadfkMCfrKUi6qr3I1Q','UCstw-41J8syXgdJ8xWvaizA','UC652oRUvX1onwrrZ8ADJRPw','UCOSIXyYdT93OzpRnAuWaKjQ','UCcgqSM4YEo5vVQpqwN-MaNw','UCuHzBCaKmtaLcRAOoazhCPA','UCwK3C8Vgphad4PweezfUBAQ','UC3jOd7GUMhpgJRBhiLzuLsg','UCO5IQ70V7l-XpHW40HwaGsw','UCy3zgWom-5AGypGX_FVTKpg','UCkntT5Je5DDopF70YUsnuEQ','UCV4UK9LNNLViFP4qZA_Wmfw','UCKC11MOR51CLg4JpYj8jb4g','UCvpDeGlR5wLP9Z3Tb6K0Xfg','UChAjsnxIhU-3WZ8aSxE650A']
    itune_id_list = [
    'the+chainsmokers', 'blackbear', 'lil+skies', 'lil+pump', 'nate+smith', 'armani+white', 'jackson+dean', 
    'jelly+roll', 'jax', 'jordan+davis', 'riley+green', 'thomas+rhett', 'lainey+wilson', 'hardy', 'lana+del+rey', 
    'britney+spears', 'elton+john', 'sia', 'kane+brown', 'brenda+lee', 'cole+swindell', 'tems', 'future', 
    'mariah+carey', 'chris+brown', 'isabel+larosa', 'chencho+corleone', 'cardi+b', 'glorilla', 'manuel+turizo', 
    'selena+gomez', 'rema', 'thxsomch', 'quavo', 'bailey+zimmerman', 'lizzo', 'glass+animals', 'the+1975', 
    'imagine+dragons', 'sabrina+carpenter', 'omar+apollo', 'joji', 'noah+kahan', 'nicky+youre', 'jvke', 
    'robin+schulz', 'oliver+tree', 'pharrell+williams', 'stephen+sanchez', 'latto', 'bebe+rexha', 'david+guetta', 
    'onerepublic', 'tate+mcrae', 'tiësto', 'meghan+trainor', 'd4vd', 'lil+nas+x', 'taylor+swift', 'drake', 
    'bad+bunny', 'kanye+west', 'the-weeknd', 'juice+wrld', 'lil+baby', 'kendrick+lamar', 'morgan+wallen', 
    'youngboy+never+broke+again', 'harry+styles', 'post+malone', '21+savage', 'doja+cat', 'j.+cole', 'eminem', 
    'xxxtentacion', 'lil+uzi+vert', 'polo+g', 'justin+bieber', 'mac+miller', 'billie+eilish', 'travis+scott', 
    'ariana+grande', 'ed+sheeran', 'bruno+mars', 'jack+harlow', 'kodak+black', 'trippie+redd', 'playboi+carti', 
    'luke+combs', 'rihanna', 'beyoncé', 'zach+bryan', 'nicki+minaj', 'sza', 'olivia+rodrigo', 'khalid', 'yeat', 
    'metro+boomin', 'sam+smith', 'kim+petras'
]


    data1 = get_spotify_data(spotify_id_list)
    data2 = get_youtube_data(youtube_id_list)
    data3 = get_itune_data(itune_id_list)

    add_name_data(artist_list, youtube_id_list, spotify_id_list, itune_id_list, cur, conn)
    add_spotify_data(data1,cur,conn)
    add_youtube_data(data2,cur,conn)
    add_itune_data(data3, cur, conn)

    list1 = youtube_total_views_rank(cur,conn)
    list2 = youtube_subscribers_rank(cur,conn)
    list3 = youtube_ave_views_rank(cur,conn)
    list4 = spotify_followers_rank(cur,conn)
    list5 = spotify_popularity_rank(cur,conn)
    list6 = spotify_genres_followers_rank(cur,conn)
   
    
    youtube_total_views_rank_chart(list1)
    youtube_subscribers_rank_chart(list2)
    spotify_followers_rank_chart(list4)
    spotify_popularity_rank_chart(list5)
    spotify_genres_followers_rank_chart(list6)

    #for itunes
    genre_ranking = itune_rank_genres_by_popularity(cur)
    track_ranking = itune_rank_ave_track_time_by_genre(cur)
    itune_visualize_genre_ranking(genre_ranking)
    itune_visualize_ave_track_time_by_genre(track_ranking)
    

    total_score_rank = count_total_scores(list1,list2,list3,list4,list5)
    total_rank_chart(total_score_rank)

    write_csv_aveview(list3, "youtube_avg_views.csv")
    write_csv_genre(list6, "spotify_genres.csv")
    write_csv_total(total_score_rank, "total_scores.csv")

main()
    

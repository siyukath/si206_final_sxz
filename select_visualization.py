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


def open_database(db_name):
    dir = os.path.dirname(__file__) + os.sep
    conn = sqlite3.connect(dir+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def itune_rank_genres_by_popularity(cur):
    cur.execute("""
        SELECT g.genre_name, COUNT(*) as track_count 
        FROM itune i
        JOIN genres g ON i.genre_id = g.genre_id
        GROUP BY g.genre_name 
        ORDER BY track_count DESC
    """)
    return cur.fetchall()

def itune_rank_ave_track_time_by_genre(cur):
    cur.execute("""
        SELECT g.genre_name, AVG(i.trackTimeMillis) as avg_track_time 
        FROM itune i
        JOIN genres g ON i.genre_id = g.genre_id
        GROUP BY g.genre_name 
        ORDER BY avg_track_time DESC
    """)
    result = cur.fetchall()
    print("Query result:", result)  # Debug line to check the query result
    return result

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
    cur.execute("SELECT genres.genre_name, SUM(spotify.followers) FROM spotify JOIN genres ON spotify.genre_id = genres.genre_id GROUP BY spotify.genre_id")
    data = cur.fetchall()
    for item in data:
        genres_dict[item[0]] = item[1]
    genres_followers_rank = sorted(genres_dict.items(), key=lambda item: item[1], reverse=True)
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
    # Unpack the tuples into separate lists
    genres, track_counts = zip(*genre_data)

    # Convert genres and track_counts to lists
    genres = list(genres)
    track_counts = list(track_counts)

    # Handle None values or convert data types if necessary
    genres = [genre if genre is not None else 'Unknown' for genre in genres]
    track_counts = [count if count is not None else 0 for count in track_counts]

    # Visualization
    plt.figure(figsize=(12, 6))
    plt.bar(genres, track_counts, color='green')
    plt.xlabel('Genre')
    plt.ylabel('Number of Tracks')
    plt.title('Most Popular Genres on iTunes')
    plt.xticks(rotation=45)
    plt.show()

def itune_visualize_ave_track_time_by_genre(track_data):
    # Unpack the tuples into separate lists
    genres, avg_track_times = zip(*track_data)

    # Convert genres and avg_track_times to lists
    genres = list(genres)
    avg_track_times = list(avg_track_times)

    # Handle None values or convert data types if necessary
    genres = [genre if genre is not None else 'Unknown' for genre in genres]
    avg_track_times = [time if time is not None else 0 for time in avg_track_times]

    # Visualization
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
     # Initialize database connection
    cur, conn = open_database('music.db')

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


# Execute main function
if __name__ == "__main__":
    main()
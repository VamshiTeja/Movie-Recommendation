# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-02-19 23:34:40
# @Last Modified by:   vamshi
# @Last Modified time: 2018-02-21 19:39:39

import sys
import os
import numpy as np
import pandas as pd
import csv
import nltk
import string

import psycopg2
from config import config

data_dir ="../Data/"

title_file = data_dir + "title.basics.tsv"
name_file = data_dir + "name.basics.tsv"
episode_file = data_dir + "title.episode.tsv"
ratings_file = data_dir + "title.ratings.tsv"
principals_file = data_dir + "title.principals.tsv"
crew_file = data_dir + "title.crew.tsv"

names = pd.read_csv(name_file, header = None,sep='\t',names=['nconst','primaryName','birthYear','deathYear','primaryProfession','knownForTitles'])
titles = pd.read_csv(title_file, header = 1,sep='\t',names=['tconst','titleType','primaryTitle','originalTitle','isAdult','startYear','endYear','runtime','genres'])

episodes = 	pd.read_csv(episode_file,header=1,sep='\t',names=['tconst','parentTconst','seasonNumber','episodeNumber'])
ratings  = pd.read_csv(ratings_file,header=1,sep='\t',names = ['tconst','averageRating','numVotes'])

crew = pd.read_csv(crew_file,header=1,sep='\t',names = ['tconst','directors','writers'])
principals = pd.read_csv(principals_file,header=1,sep='\t',names=['tconst','pricipalCast'])

print"Mapping titles to unique ids..."
ids_alphanumeric = titles.tconst.unique()
ids_dict_title = dict(zip(ids_alphanumeric,range(len(ids_alphanumeric))))
df = titles.applymap(lambda s: ids_dict_title.get(s) if s in ids_dict_title else s)

print"Mapping names to unique ids"
ids_alphanumeric = names.nconst.unique()
ids_dict_names = dict(zip(ids_alphanumeric,range(len(ids_alphanumeric))))
df_names = names.applymap(lambda s: ids_dict_names.get(s) if s in ids_dict_names else s)

print"Mapping episodes tconst to unique ids "
df_episodes = episodes.applymap(lambda s: ids_dict_title.get(s) if s in ids_dict_title else s)
df_episodes = pd.DataFrame(df_episodes)
df_episodes['parentTconst'] = df_episodes['parentTconst'].apply(lambda x : x if type(x)!=str else int(x[2:]))

print "Mapping ratings tconst to unique ids"
df_ratings = ratings.applymap(lambda s: ids_dict_title.get(s) if s in ids_dict_title else s)

print"Mapping crew tconst"
#df_crew = crew.applymap(lambda s: ids_dict_title.get(s) if s in ids_dict_title else s)

print"Mapping principals tconst"
#df_principals = principals.applymap(lambda s: ids_dict_title.get(s) if s in ids_dict_title else s)

#Differentiate between movies and Tv Series
dg = pd.DataFrame({'count' : df.groupby("titleType").size()}).reset_index()
print("movie types and their counts", dg )

print("Removing unnecessary movie types")
df = df[df.titleType != "videoGame"]
df = df[df.titleType != "video"]
df = df[df.titleType != "short"]


dg = pd.DataFrame({'count' : df.groupby("titleType").size()}).reset_index()
print("after removing movie types and their counts", dg )


#seperate movies and TV series
movie_dat1 = df[df.titleType == "movie"]
movie_dat2 = df[df.titleType =="tvMovie"]
movie_data = pd.concat([movie_dat1,movie_dat2])

#tv series data
tv_series_dat1 = df[df.titleType == "tvSeries"]
tv_series_dat2 = df[df.titleType =="tvMiniSeries"]
tv_series_data = pd.concat([tv_series_dat1,tv_series_dat2])

#tv eps data
tv_eps_data   = df[df.titleType == "tvEpisode"]

#Unrelated things
#tv_short_data = df[df.titleType == "tvShort"]
#tv_special    = df[df.titleType == "tvSpecial"]

'''
#extract unique genres
genres = df['genres']
genres = genres.as_matrix()
processed_genres = []
uniqueGenres = [] 

for g in  genres:
	g = str(g)
	l = nltk.word_tokenize(g)
	for gen in l:
		if not gen in uniqueGenres:
			if(gen != "," and gen !="\\N" and gen !="nan"):
				uniqueGenres.append(gen)

#create genre dataframe
genre_df = pd.DataFrame(uniqueGenres)
genre_df.to_csv("genre.csv",sep='\t')


#process professions
profes = df_names['primaryProfession']
profes = profes.as_matrix()
unique_professions = []

for g in  profes:
	g = str(g)
	l = nltk.word_tokenize(g)
	for gen in l:
		if not gen in unique_professions:
			if(gen != "," and gen !="\\N" and gen !="nan"):
				unique_professions.append(gen)

profes_df = pd.DataFrame(unique_professions)
profes_df.to_csv("professions.csv",sep='\t')
'''

#generate movie_peoplew
movie_people = df_names
movie_people = movie_people.drop('primaryProfession',1)
movie_people = movie_people.drop('knownForTitles',1)
movie_people.to_csv("movie_people.csv",index=False,sep="\t")

#generate movie database
movies = pd.merge(movie_data,df_ratings,how= 'left',on="tconst")
movies = movies.drop('titleType',1)
movies = movies.drop('endYear',1)
movies = movies.drop('genres',1)
movies.to_csv("movies.csv",index=False,sep="\t")


#generate tv show database
tv_shows = pd.merge(tv_series_data, df_ratings,how= 'left',on="tconst")
tv_shows = tv_shows.drop('genres',1)
tv_shows = tv_shows.drop('titleType',1)
tv_shows.to_csv("tv_shows.csv",index=False,sep="\t")


#generate tv eps database
tv_eps = pd.merge(tv_eps_data, df_episodes,how='inner',on='tconst')
tv_eps['parentTconst'] = tv_eps['parentTconst'].astype('int')
tv_eps = tv_eps.drop('titleType',1)
tv_eps = tv_eps.drop('endYear',1)
tv_eps = tv_eps.drop('genres',1)
tv_eps = tv_eps.drop('isAdult',1)
tv_eps = pd.merge(tv_eps, df_ratings,how='inner',on='tconst')
tv_eps.to_csv("tv_eps.csv",index=False,sep='\t')



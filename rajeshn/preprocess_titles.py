# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-02-19 23:34:40
# @Last Modified by:   vamshi
# @Last Modified time: 2018-02-21 10:29:58

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

name_file = data_dir + "title.basics.tsv"
name_file = data_dir + "name.basics.tsv"
episode_file = data_dir + "title.episode.tsv"
ratings_file = data_dir + "title.ratings.tsv"
principals_file = data_dir + "title.principals.tsv"
crew_file = data_dir + "title.crew.tsv"


names = pd.read_csv(name_file, header = None,sep='\t',names=['nconst','primaryName','birthYear','deathYear','primaryProfession','knownForTitles'])
titles = pd.read_csv(name_file, header = 1,sep='\t',names=['tconst','titleType','primaryTitle','originalTitle','isAdult','startYear','endYear','runtime','genres'])

episodes = 	pd.read_csv(episode_file,header=1,sep='\t',names=['tconst','parentTconst','seasonNumber','episodeNumber'])
ratings  = pd.read_csv(ratings_file,header=1,sep='\t',names = ['tconst','averageRating','numVotes'])

crew = pd.read_csv(crew_file,header=1,sep='\t',names = ['tconst','directors','writers'])
principals = pd.read_csv(principals_file,header=1,sep='\t',names=['tconst','pricipalCast'])

print"Mapping titles to unique ids..."
ids_alphanumeric = titles.tconst.unique()
ids_dict_title = dict(zip(ids_alphanumeric,range(len(ids_alphanumeric))))
df = titles.applymap(lambda s: ids_dict_title.get(s) if s in ids_dict else s)

print"Mapping names to unique ids"
ids_alphanumeric = names.nconst.unique()
ids_dict_names = dict(zip(ids_alphanumeric,range(len(ids_alphanumeric))))
df_names = names.applymap(lambda s: ids_dict_names.get(s) if s in ids_dict else s)

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
tv_short_data = df[df.titleType == "tvShort"]
tv_special    = df[df.titleType == "tvSpecial"]

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

print(uniqueGenres)


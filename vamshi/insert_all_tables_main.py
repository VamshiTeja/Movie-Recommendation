# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-02-19 23:34:40
# @Last Modified by:   vamshi
# @Last Modified time: 2018-02-21 18:25:41

import sys
import os
import numpy as np
import pandas as pd
import csv

import psycopg2
from config import config

data_dir ="../Data/"

name_file = data_dir + "name.basics.tsv"

#titles = pd.read_csv(name_file, header = None,sep='\t',names=['nconst','primaryName','birthYear','deathYear','primaryProfession','knownForTitles'])

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (

        """
        CREATE TABLE MOVIE (
            movieID INTEGER PRIMARY KEY,
            primary_title VARCHAR(255),
            original_title VARCHAR(255),
            isAdult boolean,
            release_year VARCHAR(255),
            run_time VARCHAR(255),
            avg_rating DECIMAL,
            num_votes DECIMAL
            )
        """,
        """
        CREATE TABLE GENRE (
            genreID SERIAL PRIMARY KEY,
            genre VARCHAR(225)
        )
        """,

        """
        CREATE TABLE TV_SHOW(
        	showID INTEGER PRIMARY KEY,
            primary_title VARCHAR(255),
            original_title VARCHAR(255),
            isAdult boolean,
            start_year VARCHAR(255),
            end_year VARCHAR(255),
            run_time VARCHAR(255),
            avg_rating DECIMAL,
            num_votes DECIMAL
        )
        """,
        """
        CREATE TABLE TV_episode(
        	epsoID INTEGER PRIMARY KEY,
            primary_title VARCHAR(255),
            original_title VARCHAR(255),
            release_year VARCHAR(255),
            run_time VARCHAR(255),
            showID INTEGER REFERENCES TV_SHOW,
            season_No VARCHAR(255),
            epso_No VARCHAR(255),
            avg_rating DECIMAL,
            num_votes DECIMAL
        	)
        """,
        """
        CREATE TABLE DIRECTOR(
        	dirID INTEGER PRIMARY KEY
        )
        """,
        """
        CREATE TABLE WRITER(
        	wriID INTEGER PRIMARY KEY
        )
        """,
        """
        CREATE TABLE PRINCIPALS(
        	perID INTEGER PRIMARY KEY,
        	job_category VARCHAR(255)  
        )
        """,

        """
        CREATE TABLE MOVIE_PEOPLE (
            PersonID VARCHAR(255) PRIMARY KEY,
            Name VARCHAR(255),
            birthYear VARCHAR(255),
            deathYear VARCHAR(255)
        )
        """

          
        )

    copy_commands = ("""COPY MOVIE FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/movies.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY TV_SHOW FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/tv_shows.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY TV_episode FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/tv_eps.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY MOVIE_PEOPLE FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/movie_people.csv' DELIMITER '\t' CSV HEADER""",
    				)

    c = ("""CREATE TEMP TABLE  temp_table (id bigint, dob bigint )""",
    	"""COPY temp_table '/home/vamshi/BTECH/SEM 6/DBMS II/Project/Data/name.basics.tsv' delimiter '\t'  csv header""",
    	"""UPDATE movie_People SET movie.dob = temp_table.dob FROM temp_table  Where original_table.id = temp_table.id""",
    	"""DROP TABLE temp_table""")

    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
        	cur.execute(command)
        for cmd in copy_commands:
        	cur.execute(cmd)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 
 
if __name__ == '__main__':
    create_tables()

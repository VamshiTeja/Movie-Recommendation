# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-02-19 23:34:40
# @Last Modified by:   vamshi
# @Last Modified time: 2018-02-23 23:19:07

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
            genres VARCHAR(255),
            avg_rating DECIMAL,
            num_votes DECIMAL,
            directors VARCHAR(255),
            writers VARCHAR(255)
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
            genres VARCHAR(255),
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
        CREATE TABLE MOVIE_PEOPLE (
            PerID INTEGER PRIMARY KEY,
            Name VARCHAR(255),
            birthYear VARCHAR(255),
            deathYear VARCHAR(255),
            primaryProfession VARCHAR(255),
            knownforTitle VARCHAR(255)
        )
        """,
        """
        CREATE TABLE PRINCE_CAST(
            movID INTEGER REFERENCES MOVIE,
            perID INTEGER REFERENCES MOVIE_PEOPLE,
            category VARCHAR(255),
            job VARCHAR(255),
            characters VARCHAR(255)

        )
        """,
        )

    copy_commands = ("""COPY MOVIE FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/movies.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY TV_SHOW FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/tv_shows.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY TV_episode FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/tv_eps.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY MOVIE_PEOPLE FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/movie_people.csv' DELIMITER '\t' CSV HEADER""",
    				"""COPY PRINCE_CAST FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/asg2/prince_cast.csv' DELIMITER '\t' CSV HEADER""",
    				)

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

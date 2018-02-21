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

import psycopg2
from config import config

data_dir ="../Data/"

name_file = data_dir + "name.basics.tsv"

#titles = pd.read_csv(name_file, header = None,sep='\t',names=['nconst','primaryName','birthYear','deathYear','primaryProfession','knownForTitles'])

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE movie_people (
            PersonID VARCHAR(255) PRIMARY KEY,
            Name VARCHAR(255),
            birthYear VARCHAR(255),
            deathYear VARCHAR(255),
            primaryProfession VARCHAR(255),
            knownForTitles VARCHAR(255)
        )
        """,
        """
        CREATE TABLE movie (
            MovieID INTEGER PRIMARY KEY,
            release_date VARCHAR(255),
            run_time INTEGER,
            title VARCHAR(255)
        )
        """)

    copy_commands = ("""COPY movie_people FROM '/home/vamshi/BTECH/SEM 6/DBMS II/Project/Data/name.basics.tsv' DELIMITER '\t' """)

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
        
        cur.execute(copy_commands)
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
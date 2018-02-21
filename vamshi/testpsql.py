#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-02-14 17:22:54
# @Last Modified by:   vamshi
# @Last Modified time: 2018-02-14 22:12:35

import os 
import sys
import psycopg2
from config import config
 
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        
        for key in params.keys():
                params[key] = unicode(params[key]).encode("utf-8")
        
        params = {k: unicode(v).encode("utf-8") for k,v in params.iteritems()}
        params = {unicode(k).encode("utf-8"): v for k,v in params.iteritems()}
        
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
 
        # create a cursor
        cur = conn.cursor()
        
 # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 		
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
     # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
 
if __name__ == '__main__':
	connect()
# -*- coding: utf-8 -*-
# @Author: vamshi
# @Date:   2018-02-19 23:35:09
# @Last Modified by:   vamshi
# @Last Modified time: 2018-02-20 19:01:28
import sys
import os
import numpy as np
import pandas as pd
import csv

import psycopg2
from config import config

data_dir ="../Data/"

name_file = data_dir + "name.basics.tsv"
titles = pd.read_csv(name_file, header = None,sep='\t',names=['nconst','primaryName','birthYear','deathYear','primaryProfession','knownForTitles'])


print"Mapping names to unique ids..."
ids_alphanumeric = titles.nconst.unique()
ids_dict = dict(zip(ids_alphanumeric,range(len(ids_alphanumeric))))
print(ids_dict)
df = titles.applymap(lambda s: ids_dict.get(s) if s in ids_dict else s)

print df['nconst']
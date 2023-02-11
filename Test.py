from pymongo import MongoClient
import pymongo
import pandas as pd
import threading
from json import loads
import numpy as np
import math
import json
import time
import concurrent.futures
from pymongo import MongoClient


panelafterincumbent = pd.read_csv("~/ExemplarincumbentpanelDID_0130.csv",
sep=",")

applist = panelafterincumbent['appID'].drop_duplicates()
exemplarid = panelafterincumbent['exemplarID'].drop_duplicates()


#get the description summarize: 

client = MongoClient()
dbname = client['gplayall']

allappdescrip = pd.DataFrame()
exemplardata = pd.DataFrame()
for i in range(1,13,1):
    print(i)
    namedata = 'moredescribe'+str(i)
    collection = dbname[namedata]
    item_details = collection.find()
    allmonthlydata = pd.DataFrame(item_details)
    #get appdescriptions
    month= allmonthlydata[allmonthlydata['appId'].isin(applist)]
    month = month[['appId','corpus','dictionary','month']]
    allappdescrip = pd.concat([allappdescrip,month],axis =0)

    month= allmonthlydata[allmonthlydata['appId'].isin(exemplarid)]
    month = month[['appId','corpus','dictionary','month']]
    exemplardata = pd.concat([exemplardata,month],axis =0)

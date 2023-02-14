#set up the training, testing and predicting file

import pymongo
import pandas as pd
from json import loads
import numpy as np
import math
import json
import time
from pymongo import MongoClient

import tqdm                                                                                                   
import numpy as np
import pandas as pd


#find pair of the apps

panelafterincumbent= pd.read_csv("~/Fulldataset.csv",sep=",")
pairedapp = panelafterincumbent[['appID','exemplarID']]

pairedapp =pairedapp.drop_duplicates()

#select 2000 randomly
samplelabel = pairedapp.sample(n = 2000)

#merge with the description 
client = MongoClient()
dbname = client['gplayall']
def keepone(input):
    return(input.iloc[0])
  
applist= samplelabel['appID'].values.tolist()
applist2 = applist + samplelabel['exemplarID'].values.tolist()

applist2= list( dict.fromkeys(applist2) ) 



i =1
namedata = 'basicinfo'+str(i)
collection = dbname[namedata]
item_details = collection.find()
allmonthlydata = pd.DataFrame(item_details)

#select the needed descriptions

monthdata= allmonthlydata[allmonthlydata['appId'].isin(applist2)]
monthdata = monthdata.groupby(['appId']).apply(keepone)
monthdata.drop_duplicates(keep='first',ignore_index=True)
monthdata.reset_index(drop = True, inplace = True)
monthdata = monthdata[['appId','description']]

#merge with the two sample
samplelabel1 = samplelabel.merge(monthdata, left_on ="appID", right_on ="appId", how="left")
samplelabel1 = samplelabel1.merge(monthdata, left_on ="exemplarID", right_on ="appId", how="left")

samplelabel1.to_csv("~/codingsubsticom.csv", sep=",")
#hand code to get the substitute and complements



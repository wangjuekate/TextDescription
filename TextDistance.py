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


panelafterincumbent = pd.read_csv("/Users/jjw6286/Nextcloud/AppReposion_Wenpin/TopicanalysesMonthly/ExemplarincumbentpanelDID_0130.csv",
sep=",")

applist = panelafterincumbent['appID'].drop_duplicates()
exemplarid = panelafterincumbent['exemplarID'].drop_duplicates()


#get the description summarize: 

client = MongoClient()
dbname = client['gplayall']

allappdescrip = pd.DataFrame()
exemplardata = pd.DataFrame()
for i in range(1,13,1):
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


print(exemplardata['month'].value_counts())

https://stackoverflow.com/questions/69623895/pymongo-errors-cursornotfound-cursor-id-not-found
cursor = col.find({}, no_cursor_timeout=True, batch_size=1)
for x in cursor:
  print(x)

cursor.close()  # <- Don't forget to close the cursor



def caldistance(corpusa, corpusb,dicta, dictb ):
    listworda =pd.DataFrame()
    for id, freq in corpusa:
        itemdf = pd.DataFrame([dicta[id],freq])
        listworda = pd.concat([listworda, itemdf.transpose()],axis =0)
    listworda.columns =['word','freq']

    listwordb =pd.DataFrame()
    for id, freq in corpusb:
        itemdf = pd.DataFrame([dictb[id],freq])
        listwordb = pd.concat([listwordb, itemdf.transpose()],axis =0)
    listwordb.columns=['word','freq']

    total_merge = listworda.merge(listwordb, on='word', how='outer', indicator=True)
    total_merge= total_merge.fillna(0)
    distance = sum((total_merge['freq_x']- total_merge['freq_y'])**2)
    return(distance)

#each app calculate the distance between app and exemplars
print(panelafterincumbent.columns.values)
'''['Unnamed: 0' 'X' 'Unnamed..0' 'scrapTime' 'mininstall' 'genreId_x'
 'reviews_x' 'score' 'price' 'free' 'developerId' 'updated' 'month'
 'appID' 'after' 'exemplarID' 'changemonth' 'newID' 'bridges' 'defense'
 'allwords' 'bridging' 'defensing' 'allwordsfill' 'logreview' 'logbridge'
 'logdefense' 'logmininstall' 'bridgedummy' 'defencedummy' 'dummyfree'
 'reviews_y' 'genreId_y' 'substitue']'''


for index, row in panelafterincumbent.iterrows():
    appid = [row['appID']]
    exemplarid =  [row['exemplarID']]
    month = row['month']
    try:
        aside= allappdescrip[(allappdescrip['appId'].isin(appid) &  allappdescrip['month']==month)]
        corpusa = aside.iloc[0,1]
        dicta = aside.iloc[0,2]
        bside= allappdescrip[(allappdescrip['appId'].isin(exemplarid) &  allappdescrip['month']==month)]
        corpusb = bside.iloc[0,1]
        dictb = bside.iloc[0,2]
        distance = caldistance(corpusa, corpusb,dicta, dictb )
        panelafterincumbent.loc[index, 'distance' ] =  distance
    except: 
        panelafterincumbent.loc[index, 'distance' ] =  0

panelafterincumbent.to_csv('/Users/jjw6286/Nextcloud/AppReposion_Wenpin/TopicanalysesMonthly/ExemplarincumbentDID0210.csv', 
sep=',',index=False)

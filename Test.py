
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
import concurrent.futures
import multiprocessing
num_processes = multiprocessing.cpu_count()

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

def extract(appid,exemplarid,month):
        appid = [appid]
        exemplarid =  [exemplarid]
        month = month
        try: 
            aside= allappdescrip[(allappdescrip['appId'].isin(appid)) &  (allappdescrip['month']==month)]
            print(aside)
            corpusa = aside.iloc[0,1]
            dicta = aside.iloc[0,2]
            bside= allappdescrip[(allappdescrip['appId'].isin(exemplarid)) &  (allappdescrip['month']==month)]
            print(bside)
            corpusb = bside.iloc[0,1]
            dictb = bside.iloc[0,2]
            distance = caldistance(corpusa, corpusb,dicta, dictb )
            return(distance)

        except:
            return(0)

#access database
client = MongoClient()
dbname = client['gplayall']


for i in range(2,13,1):

    namedata = 'moredescribe'+str(i)
    collection = dbname[namedata]
    item_details = collection.find()
    allmonthlydata = pd.DataFrame(item_details)
    print(allmonthlydata.iloc[1])
    print(allmonthlydata['month'].value_counts())

    #get app list

    panelafterincumbent = pd.read_csv("~/ExemplarincumbentpanelDID_0130.csv",
    sep=",")

    applist1 = panelafterincumbent['appID'].drop_duplicates()
    exemplarid = panelafterincumbent['exemplarID'].drop_duplicates()
    applist= applist1.values.tolist() + exemplarid.values.tolist()

    #get the description summarize: 
    allappdescrip = pd.DataFrame()
    monthdata= allmonthlydata[allmonthlydata['appId'].isin(applist)]
    monthdata = monthdata[['appId','corpus','dictionary','month']]
    allappdescrip = pd.concat([allappdescrip,monthdata],axis =0)

    print(allappdescrip['month'].value_counts())

    #calculate

    test = panelafterincumbent[panelafterincumbent['month']==i]
    print(test.iloc[1])


    #set up paralle process

    with concurrent.futures.ProcessPoolExecutor(num_processes) as pool:
        test['distance'] = list(tqdm.tqdm(pool.map(extract, test['appID'], test['exemplarID'],test['month'], chunksize=10), total=test.shape[0]))
    namefile = "test"+str(i)
    test.to_csv('~/'+namefile+'.csv', sep=',',index=False)

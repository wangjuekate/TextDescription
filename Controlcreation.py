
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


panelafterincumbent= pd.read_csv("~/Fulldataset.csv",sep=",")

applist= panelafterincumbent['appID'].drop_duplicates()
def keepone(input):
    return(input.iloc[0])

#select from each one and get the relevant
client = MongoClient()
dbname = client['gplayall']
allappdescrip = pd.DataFrame()

for i in range(1,13,1):
    namedata = 'basicinfo'+str(i)
    collection = dbname[namedata]
    item_details = collection.find()
    allmonthlydata = pd.DataFrame(item_details)
    monthdata= allmonthlydata[allmonthlydata['appId'].isin(applist)]
    monthdata = monthdata.groupby(['appId']).apply(keepone)
    monthdata.drop_duplicates(keep='first',ignore_index=True)
    allappdescrip = pd.concat([allappdescrip,monthdata],axis =0)
    allappdescrip['month']=i


#calculate with group by
applevel = allappdescrip[['appId','month','size','contentRating']]
test = panelafterincumbent

exemplardata = pd.read_csv('~/Exemplarchanges.csv',
sep=",")

exemplardata1 =exemplardata[['appID','startgenre','developerId']]
print(exemplardata1.iloc[1])
test1 = test.merge(exemplardata1, left_on = ["exemplarID"], right_on = ["appID"],how ="left")
test1 = test1.drop(['appID_y','genreId_y', 'reviews_y','developerId_x'],axis =1)

test1 = test1.rename({'appID_x':'appID','genreId_x':'genreId',
'reviews_x':'reviews','developerId_y':'developerId'},axis =1)


test1 = test1.merge(applevel, left_on = ["appID","month"], right_on = ["appId","month"],how ="left")


#developer set
def developer(input):
    output = pd.DataFrame()
    review = sum(input['reviews'])

    developerid = input['developerId'].values.tolist()
    month = input['month'].values.tolist()

    test = pd.DataFrame([review,developerid[0],month[0]]).T
    print(test)
    output = pd.concat([output,test],axis =0)
    return(output)

getdeveloper = test1.groupby(['developerId','month']).apply(developer)
getdeveloper = pd.DataFrame(getdeveloper)
getdeveloper.reset_index(drop = True, inplace = True)
print(getdeveloper)
getdeveloper.columns =['developerper','developerId','month']
test1.reset_index(drop = True, inplace = True)
test1= test1.merge(getdeveloper, left_on = ["developerId","month"], right_on = ["developerId","month"],how ="left")




def category(input):
    output = pd.DataFrame()
    numberentry = len(input.index)
    categoryid = input['genreID'].values.tolist()
    month = input['month'].values.tolist()
    test = pd.DataFrame([numberentry ,categoryid[0],month[0]]).T
    print(test)
    output = pd.concat([output,test],axis =0)
    return(output)

getcategory = allappdescrip.groupby(['genreID','month']).apply(category)
getcategory = pd.DataFrame(getcategory)
getcategory.reset_index(drop = True, inplace = True)
getcategory.columns =['numreviewcateogry','genreID','month']

test1.reset_index(drop = True, inplace = True)
test1 = test1.merge(getcategory, left_on = ["genreId","month"], right_on = ["genreID","month"],how ="left")


#get the update time
test1['update_date'] = pd.to_datetime(test1['updated'])
test1['updatemonth'] = test1['update_date'].dt.month
test1['updateyear'] = test1['update_date'].dt.year

test1['updatestimelength'] = (2017- test1['updateyear'])*12+test1['month']- test1['updatemonth']

test1.to_csv("~/Fulldatawithcontrol.csv",sep=',',index=False)


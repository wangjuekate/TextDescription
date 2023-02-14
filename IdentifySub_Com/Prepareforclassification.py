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

pairedapp = panelafterincumbent[['appID','exemplarID']]

print(pairedapp.iloc[1])

pairedapp =pairedapp.drop_duplicates()

#merge all the pairs with the comments

client = MongoClient()
dbname = client['gplayall']
collection = dbname['comments1']
item_details = collection.find()
allmonthlydata = pd.DataFrame(item_details)

#attach all comments together
def attachcomments(input):
  appId = input.iloc[0,0]
  appId = str(appId)
  comment = ""
  for text in input['comments']:
    comment = comment + str(text)
  output = pd.DataFrame([appId,comment]).T
  return(output)

commentcombine = allmonthlydata.groupby('_id').apply(attachcomments)
commentcombine.reset_index(drop = True, inplace = True)

print(commentcombine.iloc[1])

commentcombine.columns = ['_id','comments']

#%%
pairedapp = pairedapp.merge(commentcombine,left_on ="appID", right_on ="_id", how="left")
pairedapp = pairedapp.merge(commentcombine,left_on ="exemplarID", right_on ="_id", how="left")

print(pairedapp.iloc[1])



pairedapp.to_csv("~/TextDescription/IdentifySub_Com/datatraining/Alltoclassify.csv",sep=",",index=False)


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

#merge with the descriptions as well

pairedapp= pd.read_csv("~/TextDescription/IdentifySub_Com/datatraining/Alltoclassify.csv",sep=",",index=False)

print(len(pairedapp.index))

client = MongoClient()
dbname = client['gplayall']
def keepone(input):
    return(input.iloc[0])

collection = dbname['basicinfo1']
item_details = collection.find()
description = pd.DataFrame(item_details)
monthdata = description.groupby(['appId']).apply(keepone)
monthdata.drop_duplicates(keep='first',ignore_index=True)
monthdata.reset_index(drop = True, inplace = True)
description = monthdata[['appId','description']]

pairedapp = pairedapp.merge(description,left_on ="appID", right_on ="appId", how="left")
pairedapp = pairedapp.merge(description,left_on ="exemplarID", right_on ="appId", how="left")

print(len(pairedapp.index))

pairedapp.to_csv("~/TextDescription/IdentifySub_Com/datatraining/Alltoclassify_des.csv",sep=",",index=False)


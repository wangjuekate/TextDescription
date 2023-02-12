
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

panel = pd.DataFrame()

for i in range(1,13,1):
    namefile = "test"+str(i)
    test = pd.read_csv('~/'+namefile+'.csv', sep=",")
    panel = pd.concat([panel,test],axis =0)

panel.to_csv("~/Fulldataset.csv",sep=",",index=False)
    



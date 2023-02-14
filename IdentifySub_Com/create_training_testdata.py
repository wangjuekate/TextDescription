

codedsample= pd.read_csv("~/codingsubsticom_update.csv",sep=",")

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

print(len(codesample.index))
codedsample = codedsample.merge(commentcombine,left_on ="appID", right_on ="appId", how="left")
codedsample = codedsample.merge(commentcombine,left_on ="exemplarID", right_on ="appId", how="left")
print(len(codesample.index))


train = codedsample.head(1000)
test = codesample.tail(1000)

train.to_csv("~/TextDescription/IdentifySub_Com/datatraining/train.csv", sep=",")
test.to_csv("~/TextDescription/IdentifySub_Com/datatraining/test.csv", sep=",")

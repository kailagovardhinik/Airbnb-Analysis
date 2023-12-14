from pymongo import MongoClient
import pandas as pd
import os
#collecting data from mongodb atlas
client = MongoClient("mongodb+srv://kailagovardhini:IQNMcd9QEEjHM0k3@airbnbanalysis.fhnjweg.mongodb.net/")
db = client["sample_airbnb"]
collection = db["listingsAndReviews"]
# Fetch data from MongoDB and convert to a list of dictionaries
list=[]
for data in collection.find({},{"id":1,"name":1,"address":1,"description":1,"price":1,"host":1,"availability":1,"amenities":1,"review_scores":1,"reviews":1}):
    list.append(data)
df1=pd.DataFrame(list,columns=['_id', 'name', 'description','price',"amenities"])
host_list=[]
for i in range(len(list)):
    host_list.append(list[i]["host"])
host=pd.DataFrame(host_list,columns=["host_id","host_name"])    
final=df1.join(host)
rating_list=[]
for i in range(len(list)):
    rating_list.append(list[i]["review_scores"])
rating=pd.DataFrame(rating_list)
final=df1.join(rating)
add=[]
for i in range(len(list)):
    add.append(list[i]["address"])
address=pd.DataFrame(add)
final=final.join(address)
rev=[]
for i in range(len(list)):
    rev.append(list[i]["reviews"])
review=pd.DataFrame(rev[0])
review=review.drop(["_id","date","listing_id"],axis=1)
final=final.join(review)
final.to_csv('G:/Project/04 Airbnb/final.csv',index=False)
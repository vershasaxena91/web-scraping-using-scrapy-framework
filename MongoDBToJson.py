from pymongo import MongoClient
from bson.json_util import dumps, loads
  
  
# Connecting to MongoDB server
# client = MongoClient('host_name',
# 'port_number')
client = MongoClient('localhost', 27017)
  
# Connecting to the database named
# GFG
mydatabase = client.amazon_scraped_product_data
   
# Accessing the collection named
# gfg_collection
mycollection = mydatabase.product_data
  
# Now creating a Cursor instance
# using find() function
cursor = mycollection.find()
  
# Converting cursor to the list 
# of dictionaries
list_cur = list(cursor)
  
# Converting to the JSON
json_data = dumps(list_cur) 
   
# Writing data to file data.json
with open('amazon_product_data.json', 'w') as file:
    file.write(json_data)
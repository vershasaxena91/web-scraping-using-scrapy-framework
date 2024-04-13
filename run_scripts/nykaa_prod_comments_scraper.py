from webscrapingapi import WebScrapingApiClient
from pymongo import MongoClient
import datetime

client = WebScrapingApiClient(api_key="em0BALD7WEXE6dYnqeaYWrxxHPXg9jmi")
conn = MongoClient()
db = conn.nykaa_marketplace_scraping_loreal_shampoo

input_prods = list(db["product_list"].find({}, {"_id": 0}))
for prod in input_prods:
    response = client.get(
        "https://www.nykaa.com/gateway-api/products/{}/reviews?pageNo=1&sort=MOST_RECENT&domain=nykaa".format(
            prod["product_id"]
        )
    )
    data = response.json()["response"]
    # try:
    #     conn = MongoClient()
    # except:
    #     print("Could not connect to MongoDB")
    # db = conn.nykaa_marketplace_scraping_loreal_shampoo

    collection = db.product_list
    collection_comment = db.product_comments

    for comm in data["reviewData"]:
        product_comments = []
        product_comments.append(
            {
                "username": comm["name"],
                "rating": comm["rating"],
                "description": comm["description"],
                "date": comm["createdOn"],
                "likes": comm["likeCount"],
            }
        )

        collection_comment.find_one_and_update(
            {"product_id": comm["childId"]},
            {
                "$push": {
                    "comments": product_comments,
                }
            },
            upsert=True,
        )
        categories = []

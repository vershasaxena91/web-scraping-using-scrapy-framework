from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")


collec = client["amazon_marketplace_scraping_shell"]["product_list"]

prods_df = pd.read_csv("Shell_Flipkart_Amazon_Match.csv")
prods_df = prods_df.fillna("")
products = []
for i in range(len(prods_df)):
    if isinstance(prods_df["ASIN"][i], str) and prods_df["ASIN"][i] != "":
        products.append(
            {
                "product_asin": prods_df["ASIN"][i],
                "product_url": "https://www.amazon.in/dp/{}".format(
                    prods_df["ASIN"][i]
                ),
                "product_category": prods_df["Category"][i],
                "product_viscosity": prods_df["Viscosity"][i],
                "product_grade": prods_df["Grade Type"][i],
                "product_size": prods_df["Pack Size(L)"][i],
                "product_brand": prods_df["Brand"][i],
                "product_variant": prods_df["Variant"][i],
            }
        )

print(len(products))
# print(products)

present = 0
added = 0

for product in products:
    existing_item = collec.find_one({"product_asin": product["product_asin"]})
    if existing_item is None:
        added += 1
    else:
        present += 1
        print("Duplicate", product)
    collec.find_one_and_update(
        {"product_asin": product["product_asin"]}, {"$set": product}, upsert=True
    )
    client_added = collec.find_one(
        {"product_asin": product["product_asin"], "clients": "Shell"}
    )
    if not client_added:
        collec.find_one_and_update(
            {"product_asin": product["product_asin"]},
            {
                "$push": {
                    "clients": "Shell",
                }
            },
        )

print(present, added)
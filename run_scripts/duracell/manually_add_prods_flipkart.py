from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")


collec = client["flipkart_marketplace_scraping_duracell"]["product_list"]

prods_df = pd.read_csv("Duracell_Flipkart_Amazon_Match.csv")
# prods_df = prods_df.fillna("")
prods_df["PID"] = prods_df["Flipkart_URL"].apply(
    lambda x: x.split("pid=")[1].split("&")[0] if isinstance(x, str) else None
)
prods_df["LID"] = prods_df["Flipkart_URL"].apply(
    lambda x: x.split("lid=")[1].split("&")[0] if isinstance(x, str) else None
)
prods_df["Marketplace"] = prods_df["Flipkart_URL"].apply(
    lambda x: x.split("marketplace=")[1].split("&")[0] if isinstance(x, str) else None
)
prods_df["static"] = prods_df["Flipkart_URL"].apply(
    lambda x: x.split("?")[0] if isinstance(x, str) else None
)
# prods_df['Marketplace'] = "FLIPKART"
# prods_df['LID'] = ""
products = []

# prods_df.to_csv('Duracell_Flipkart_Amazon_Match.csv', index=False)
for i in range(len(prods_df)):
    if isinstance(prods_df["PID"][i], str) and prods_df["PID"][i] != "":
        products.append(
            {
                "product_pid": prods_df["PID"][i],
                "product_lid": prods_df["LID"][i],
                "marketplace": prods_df["Marketplace"][i],
                "product_url": "{}?pid={}&lid={}&marketplace={}".format(
                    prods_df["static"][i],
                    prods_df["PID"][i],
                    prods_df["LID"][i],
                    prods_df["Marketplace"][i],
                ),
            }
        )
print(len(products))
# print(products)

present = 0
added = 0

for product in products:
    existing_item = collec.find_one(
        {
            "product_pid": product["product_pid"],
            "marketplace": product["marketplace"],
        }
    )
    if existing_item is None:
        added += 1
    else:
        present += 1
        print("Duplicate", product)
    collec.find_one_and_update(
        {
            "product_pid": product["product_pid"],
            "marketplace": product["marketplace"],
        },
        {"$set": product},
        upsert=True,
    )
    client_added = collec.find_one(
        {
            "product_pid": product["product_pid"],
            "marketplace": product["marketplace"],
            "clients": "Duracell",
        }
    )
    if not client_added:
        collec.find_one_and_update(
            {
                "product_pid": product["product_pid"],
                "marketplace": product["marketplace"],
            },
            {
                "$push": {
                    "clients": "Duracell",
                }
            },
        )

print(present, added)

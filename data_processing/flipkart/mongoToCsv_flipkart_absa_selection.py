#
import json
import pandas as pd
from tqdm import tqdm

# with open('../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping_lloyd_refrigerator_11_07_22/product_comments.json', encoding='utf-8') as f:
#     prods = json.load(f)
prods = [
    json.loads(line)
    for line in open(
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/product_comments.json",
        encoding="utf8",
    )
]

print(len(prods))
# with open('park_avenue/flipkart_marketplace_scraping_park_avenue_16_02_22/product_data.json', encoding='utf-8') as f:
#     data = json.load(f)
# needed = set()
# for p in data:
#     if p['product_brand'] == "DURACELL Batteries":
#         needed.add(p['product_pid'])
# print(needed)
# print(len(needed))


comms_df = pd.DataFrame(
    columns=[
        "ASIN",
        "Marketplace",
        "username",
        "rating",
        "title",
        "date",
        "likes",
        "dislikes",
        "verified",
        "description",
    ]
)
counts = dict()
for prod in tqdm(prods):
    comms_list = []
    done = set()
    # if prod['product_asin'] not in needed:
    #     continue
    for comm in prod["comments"]:
        if str(comm) in done:
            continue
        done.add(str(comm))
        # print(comm['description'])
        if comm["description"]:
            comm["description"] = comm["description"].replace("\\n", "")
            comm["description"] = comm["description"].replace("\n", "")
            if len(comm["description"].split()) >= 5:
                comms_list.append(
                    {
                        "ASIN": prod["product_asin"],
                        "Marketplace": prod["marketplace"],
                        "username": comm["username"],
                        "rating": comm["rating"],
                        "title": comm["title"],
                        "date": comm["date"],
                        "likes": comm["likes"],
                        "dislikes": comm["dislikes"],
                        "verified": comm["verified"],
                        "description": comm["description"],
                    }
                )
    if len(comms_list) > 0:
        comms_df = comms_df.append(comms_list)
        # print(comms_df.columns)
    else:
        # print(prod)
        pass
counts = comms_df["ASIN"].value_counts().to_dict()
print(counts)
print(comms_df.shape)
comms_df = comms_df.drop_duplicates()
comms_df["Counts"] = comms_df["ASIN"].apply(lambda x: counts[x])
comms_df = comms_df.sort_values(["Counts", "ASIN"], ascending=False)
# print(comms_df)
comms_df.to_csv(
    "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_comments_for_ABSA.csv",
    index=False,
)

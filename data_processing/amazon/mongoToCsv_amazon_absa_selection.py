#
import json
import pandas as pd
from tqdm import tqdm
from google.cloud import language_v1

# with open('ceregrow/amazon_marketplace_scraping_ceregrow_13_04_22/product_comments.json', encoding='utf-8') as f:
# prods = json.load(f)

prods = [
    json.loads(line)
    for line in open(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_comments.json",
        encoding="utf8",
    )
]

print(len(prods))
# with open('ceregrow/amazon_marketplace_scraping_ceregrow_02_03_22/product_data.json', encoding='utf-8') as f:
#     data = json.load(f)
# needed = set()
# for p in data:
#     if p['product_brand'].lower() == "ceregrow" or p['product_name'].split()[0].lower() == "ceregrow":
#         needed.add(p['product_asin'])
# print(needed)
# print(len(needed))
import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "../../google_application_credentials/wtc-amazon-scrp-6484791fa61c.json"


def get_entity_sentiment(comm):
    client = language_v1.LanguageServiceClient()
    type_ = language_v1.Document.Type.PLAIN_TEXT
    language = "en"
    document = {"content": comm, "type_": type_, "language": language}
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_entity_sentiment(
        request={"document": document, "encoding_type": encoding_type}
    )

    return response


comms_df = pd.DataFrame(
    columns=[
        "ASIN",
        "username",
        "rating",
        "title",
        "date",
        "country",
        "helpful",
        "description",
    ]
)
counts = dict()
for prod in tqdm(prods):
    comms_list = []
    done = set()
    # if prod['product_asin'] not in needed:
    #     continue
    for comm in prod["comments"][0:10]:
        if str(comm) in done:
            continue
        done.add(str(comm))
        # print(comm['description'])
        if comm["description"]:
            comm["description"] = comm["description"].replace("\\n", "")
            comm["description"] = comm["description"].replace("\n", "")
            if len(comm["description"].split()) >= 5:
                entity_sentiment = get_entity_sentiment(comm["description"])
                for entity in entity_sentiment.entities:
                    comms_list.append(
                        {
                            "ASIN": prod["product_asin"],
                            "username": comm["username"],
                            "rating": comm["rating"],
                            "title": comm["title"],
                            "date": comm["date"],
                            "country": comm["country"],
                            "helpful": comm["helpful"],
                            "description": comm["description"],
                            "keyword": entity.name,
                            "importance": entity.salience,
                            "sentiment_score": entity.sentiment.score,
                            "magnitude": entity.sentiment.magnitude,
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
# comms_df['Counts'] = comms_df['ASIN'].apply(lambda x: counts[x])
comms_df = comms_df.sort_values(["ASIN"], ascending=False)
# print(comms_df)
comms_df.to_csv(
    "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_absa.csv",
    index=False,
)

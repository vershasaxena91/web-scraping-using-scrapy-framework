#
import json
import pandas as pd
from tqdm import tqdm
from google.cloud import language_v1

prods = [
    json.loads(line)
    for line in open(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/product_comments.json",
        encoding="utf8",
    )
]

print(len(prods))

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
        "likes",
        "description",
    ]
)
counts = dict()
for prod in tqdm(prods):
    comms_list = []
    done = set()

    for comm in prod["comments"][0:10]:
        if str(comm) in done:
            continue
        done.add(str(comm))

        if comm["description"]:
            comm["description"] = comm["description"].replace("\\n", "")
            comm["description"] = comm["description"].replace("\n", "")
            if len(comm["description"].split()) >= 5:
                entity_sentiment = get_entity_sentiment(comm["description"])
                for entity in entity_sentiment.entities:
                    comms_list.append(
                        {
                            "ASIN": prod["product_id"],
                            "username": comm["username"],
                            "rating": comm["rating"],
                            "title": comm["title"],
                            "date": comm["date"],
                            "likes": comm["likes"],
                            "description": comm["description"],
                            "keyword": entity.name,
                            "importance": entity.salience,
                            "sentiment_score": entity.sentiment.score,
                            "magnitude": entity.sentiment.magnitude,
                        }
                    )
    if len(comms_list) > 0:
        comms_df = comms_df.append(comms_list)

    else:

        pass
counts = comms_df["ASIN"].value_counts().to_dict()
print(counts)
print(comms_df.shape)
comms_df = comms_df.drop_duplicates()

comms_df = comms_df.sort_values(["ASIN"], ascending=False)

comms_df.to_csv(
    "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_absa.csv",
    index=False,
)

import pandas as pd
import json

if __name__ == "__main__":
    sentiments = pd.read_csv(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prods_with_ABSA.csv"
    )
    with open(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/absa_classes_nycil_powder.json"
    ) as f:
        classes = json.load(f)

    keyword_to_aspect = {}
    for c in classes:
        for k in c["Keywords"]:
            keyword_to_aspect[k] = c["Aspect"]

    sentiments["aspect"] = sentiments["keyword"].apply(
        lambda x: keyword_to_aspect.get(x.lower(), None)
    )
    print(sentiments)
    sentiments.to_csv(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_absa.csv",
        index=False,
    )

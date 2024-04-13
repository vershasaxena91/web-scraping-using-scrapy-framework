import json
import pandas as pd


data = pd.DataFrame(
    pd.read_csv(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/keywords.csv"
    )
)
aspect_map = {}
for i in range(len(data)):
    aspect_map[data.iloc[i]["Aspect"]] = aspect_map.get(data.iloc[i]["Aspect"], [])
    aspect_map[data.iloc[i]["Aspect"]].append(data.iloc[i]["keyword"])

aspect_list = []
for k, v in aspect_map.items():
    aspect_list.append({"Aspect": k, "Keywords": v})

with open(
    "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/absa_classes_nycil_powder.json",
    "w",
) as f:
    f.write(json.dumps(aspect_list))

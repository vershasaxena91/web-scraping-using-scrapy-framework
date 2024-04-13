from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
from tqdm import tqdm


class Converter_sos:
    def __init__(self, list_file, data_file) -> None:

        self.list = [json.loads(line) for line in open(list_file, encoding="utf8")]
        self.data = [json.loads(line) for line in open(data_file, encoding="utf8")]

        self.sos_df = pd.DataFrame(
            columns=[
                "Keyword",
                "Rank",
                "ASIN",
                "Title",
                "Brand",
                "Latest Selling Price",
                "Original Price",
            ]
        )
        self.reverse_asin = {}
        for prod in self.data:
            self.reverse_asin[prod["product_id"]] = prod

        print(len(self.reverse_asin))
        asins = set()
        for d in self.list:
            for p in d["product_order"]:
                asins.add(p["product_id"])
        print(len(asins))

    def convert_doc(self, doc):

        doc["product_order"] = sorted(
            doc["product_order"], key=lambda x: x["product_rank"]
        )

        sos_rank_list = []
        print(doc["keyword"], len(doc["product_order"]))
        i = 0
        present = set()

        for prod in doc["product_order"]:
            try:
                i += 1
                present.add(prod["product_id"])
                sos_rank_list.append(
                    {
                        "Keyword": doc["keyword"],
                        "Rank": i,
                        "ASIN": prod["product_id"],
                        "Title": self.reverse_asin[prod["product_id"]]["product_name"],
                        "Brand": " ".join(
                            self.reverse_asin[prod["product_id"]]["product_brand"]
                            .lower()
                            .split()
                        ).title(),
                        "Latest Selling Price": self.reverse_asin[prod["product_id"]][
                            "product_sale_price"
                        ],
                        "Original Price": self.reverse_asin[prod["product_id"]][
                            "product_original_price"
                        ],
                        "Present": True,
                    }
                )
            except:
                print(prod["product_id"])

        for asin, prod in self.reverse_asin.items():
            if asin not in present:
                sos_rank_list.append(
                    {
                        "Keyword": doc["keyword"],
                        "Rank": -1,
                        "ASIN": asin,
                        "Title": prod["product_name"],
                        "Brand": " ".join(
                            prod["product_brand"].lower().split()
                        ).title(),
                        "Latest Selling Price": prod["product_sale_price"],
                        "Original Price": prod["product_original_price"],
                        "Present": False,
                    }
                )
        return sos_rank_list

    def convert(self):
        def convert_string_to_day(sday):
            return datetime.strptime(sday, "%Y-%m-%d %H:%M:%S")

        data_old = []
        data_last = []
        seen = set()
        self.list = sorted(
            self.list, key=lambda x: convert_string_to_day(x["time"]), reverse=True
        )
        for doc in self.list:
            if doc["keyword"] in seen:

                data_old.append(doc)

            else:
                data_last.append(doc)

        print(len(data_old), len(data_last))

        for i in tqdm(range(len(data_old))):

            sos_keyword = self.convert_doc(data_old[i])

        data_last = sorted(data_last, key=lambda x: x["time"], reverse=True)
        keywords = set()
        for i in tqdm(range(len(data_last))):

            sos_keyword = self.convert_doc(data_last[i])
            if data_last[i]["keyword"] not in keywords:
                self.sos_df = self.sos_df.append(sos_keyword, ignore_index=True)
            keywords.add(data_last[i]["keyword"])

    def save(self, sos_file):
        self.sos_df.to_csv(sos_file, index=False)


if __name__ == "__main__":

    cvt = Converter_sos(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/share_of_search.json",
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/sos_data.json",
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_sos.csv"
    )

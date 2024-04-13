from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
from tqdm import tqdm


class Converter_sos:
    def __init__(self, list_file, data_file) -> None:
        # with open(list_file, encoding='utf-8') as f:
        #     self.list = json.loads(f.read())
        # with open(data_file, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
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
                "Fulfilled",
            ]
        )
        self.reverse_asin = {}
        for prod in self.data:
            self.reverse_asin[prod["product_pid"]] = prod
            try:
                if prod["product_brand"] is None or prod["product_brand"] == "NA":
                    self.reverse_asin[prod["product_pid"]]["product_brand"] = prod[
                        "product_name"
                    ].split()[0]
            except:
                pass

        print(len(self.reverse_asin))
        asins = set()
        for d in self.list:
            for p in d["product_order"]:
                asins.add(p["product_pid"])
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
                present.add(prod["product_pid"])
                # if i%3 > 0:
                #     continue
                print(
                    prod["product_pid"],
                    self.reverse_asin[prod["product_pid"]].get("product_category", []),
                )
                if (
                    self.reverse_asin[prod["product_pid"]].get("product_category", [])
                    is None
                ):
                    category = []
                else:
                    category = (
                        [None, None]
                        + self.reverse_asin[prod["product_pid"]].get(
                            "product_category", []
                        )
                    )[-2]
                print(prod["product_pid"], category)
                sos_rank_list.append(
                    {
                        "Keyword": doc["keyword"],
                        "Rank": prod["product_rank"],  # /3,
                        "ASIN": prod["product_pid"],
                        "Title": self.reverse_asin[prod["product_pid"]]["product_name"],
                        "Brand": " ".join(
                            [
                                w if w not in category else ""
                                for w in self.reverse_asin[prod["product_pid"]][
                                    "product_brand"
                                ].split()
                            ]
                        ).title(),  # " ".join(self.reverse_asin[prod['product_pid']]['product_brand'].lower().split()).title(),
                        "Latest Selling Price": self.reverse_asin[prod["product_pid"]][
                            "product_sale_price"
                        ],
                        "Original Price": self.reverse_asin[prod["product_pid"]][
                            "product_original_price"
                        ],
                        "Fulfilled": self.reverse_asin[prod["product_pid"]][
                            "product_assured"
                        ],
                        # 'Sponsored': prod['sponsored'],
                        "Present": True,
                    }
                )
            except:
                print("Failed:", prod["product_pid"])
                print(self.reverse_asin[prod["product_pid"]]["product_name"])
                print(self.reverse_asin[prod["product_pid"]]["product_brand"])
                print(self.reverse_asin[prod["product_pid"]]["product_sale_price"])
                print(self.reverse_asin[prod["product_pid"]]["product_original_price"])
                print(self.reverse_asin[prod["product_pid"]]["product_assured"])

        for asin, prod in self.reverse_asin.items():
            if asin not in present:
                try:
                    sos_rank_list.append(
                        {
                            "Keyword": doc["keyword"],
                            "Rank": -1,  # prod['product_rank'],
                            "ASIN": asin,
                            "Title": prod["product_name"],
                            "Brand": " ".join(
                                prod["product_brand"].lower().split()
                            ).title(),
                            "Latest Selling Price": prod["product_sale_price"],
                            "Original Price": prod["product_original_price"],
                            "Fulfilled": prod["product_assured"],
                            # 'Sponsored': prod['sponsored'],
                            "Present": False,
                        }
                    )
                except:
                    print("Failed: ", prod["product_pid"])

        return sos_rank_list

    def convert(self):
        def convert_string_to_day(sday):
            return datetime.strptime(sday, "%Y-%m-%d %H:%M:%S")
            # .strftime("%d-%m-%Y")

        # self.list = sorted(self.list, key=lambda x: x['time'])
        data_old = []
        data_last = []
        seen = set()
        self.list = sorted(
            self.list, key=lambda x: convert_string_to_day(x["time"]), reverse=True
        )
        for doc in self.list:
            if doc["keyword"] in seen:
                # if (datetime.today() - datetime.strptime(doc['time'], "%Y-%m-%d %H:%M:%S")).days > 1:
                data_old.append(doc)
            # if convert_string_to_day(doc['time']) == (datetime.today() - timedelta(days=0)).strftime("%d-%m-%Y"):
            else:
                data_last.append(doc)

        print(len(data_old), len(data_last))
        # data_old = sorted(data_old, key=lambda x: x['time'])
        for i in tqdm(range(len(data_old))):
            # print(data_old[i]['time'])
            sos_keyword = self.convert_doc(data_old[i])

        data_last = sorted(data_last, key=lambda x: x["time"], reverse=True)
        keywords = set()
        for i in tqdm(range(len(data_last))):
            # print(data_last[i]['time'])
            sos_keyword = self.convert_doc(data_last[i])
            if data_last[i]["keyword"] not in keywords:
                self.sos_df = self.sos_df.append(sos_keyword, ignore_index=True)
            keywords.add(data_last[i]["keyword"])

    def save(self, sos_file):
        self.sos_df.to_csv(sos_file, index=False)
        # print(self.sos_df)


if __name__ == "__main__":
    # cvt = Converter_sos('flipkart_marketplace_scraping_havells_28_01_22/share_of_search.json', 'flipkart_marketplace_scraping_havells_28_01_22/sos_data.json')
    # cvt.convert()
    # cvt.save('./flipkart_sos.csv')

    cvt = Converter_sos(
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/share_of_search.json",
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/sos_data.json",
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_sos.csv"
    )

    # cvt = Converter_sos(
    #     "../../data/InputData/lloyd_washing_machine/flipkart_marketplace_scraping_lloyd_washing_machine_14_07_22/share_of_search.json",
    #     "../../data/InputData/lloyd_washing_machine/flipkart_marketplace_scraping_lloyd_washing_machine_14_07_22/sos_data.json",
    # )
    # cvt.convert()
    # cvt.save(
    #     "../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping_11_07_22/flipkart_sos.csv"
    # )

    # cvt = Converter_sos('../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping_lloyd_refrigerator_14_07_22/share_of_search.json', '../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping_lloyd_refrigerator_14_07_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping_11_07_22/flipkart_sos.csv')

    # cvt = Converter_sos('../../data/InputData/lloyd_ac/flipkart_marketplace_scraping_lloyd_ac_14_07_22/share_of_search.json', '../../data/InputData/lloyd_ac/flipkart_marketplace_scraping_lloyd_ac_14_07_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping_11_07_22/flipkart_sos.csv')

    # cvt = Converter_sos('../../data/InputData/havells_light/flipkart_marketplace_scraping_havells_light_14_07_22/share_of_search.json', '../../data/InputData/havells_light/flipkart_marketplace_scraping_havells_light_14_07_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../../data/OutputData/havells_light/flipkart_marketplace_scraping_11_07_22/flipkart_sos.csv')

    # cvt = Converter_sos('../pureit/flipkart_marketplace_scraping_pureit_16_05_22/share_of_search.json', '../pureit/flipkart_marketplace_scraping_pureit_16_05_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../pureit/flipkart_sos.csv')

    # cvt = Converter_sos('../shell/flipkart_marketplace_scraping_shell_14_02_22/share_of_search.json', '../shell/flipkart_marketplace_scraping_shell_14_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../shell/flipkart_sos.csv')

    # cvt = Converter_sos('../duracell/flipkart_marketplace_scraping_duracell_16_02_22/share_of_search.json', '../duracell/flipkart_marketplace_scraping_duracell_16_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../duracell/flipkart_sos.csv')

    # cvt = Converter_sos('../havells_fans/flipkart_marketplace_scraping_havells_fans_03_03_22/share_of_search.json', '../havells_fans/flipkart_marketplace_scraping_havells_fans_03_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../havells_fans/flipkart_sos.csv')

    # cvt = Converter_sos('../havells_ac/flipkart_marketplace_scraping_havells_ac_04_03_22/share_of_search.json', '../havells_ac/flipkart_marketplace_scraping_havells_ac_04_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../havells_ac/flipkart_sos.csv')

    # cvt = Converter_sos('../ghadi/flipkart_marketplace_scraping_ghadi_05_03_22/share_of_search.json', '../ghadi/flipkart_marketplace_scraping_ghadi_05_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ghadi/flipkart_sos.csv')

    # cvt = Converter_sos('../kohler/flipkart_marketplace_scraping_kohler_11_03_22/share_of_search.json', '../kohler/flipkart_marketplace_scraping_kohler_11_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../kohler/flipkart_sos.csv')

    # cvt = Converter_sos('../park_avenue/flipkart_marketplace_scraping_park_avenue_19_03_22/share_of_search.json', '../park_avenue/flipkart_marketplace_scraping_park_avenue_19_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../park_avenue/flipkart_sos.csv')

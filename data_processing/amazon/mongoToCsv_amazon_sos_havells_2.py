from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
from tqdm import tqdm


class Converter_sos:
    def __init__(self, list_file, data_file) -> None:
        # with open(list_file, encoding='utf-8') as f:
        #     self.list = json.loads(f.read())['product_list']
        # with open(data_file, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())['product_data']
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
                "Latest Lowest Selling Price",
                "Latest Highest Selling Price",
                "Original Price",
                "Lowest Original Price",
                "Highest Original Price",
                "Fulfilled",
            ]
        )
        self.reverse_asin = {}
        for prod in self.data:
            self.reverse_asin[prod["product_asin"]] = prod
        #     # if prod['product_brand'] is None or prod['product_brand'] == "":
        #     #     if "Park Avenue" in prod['product_name']:
        #     #         self.reverse_asin[prod['product_asin']]['product_brand'] = "Park Avenue"
        #     #     elif "Set Wet" in prod['product_name']:
        #     #         self.reverse_asin[prod['product_asin']]['product_brand'] = "Set Wet"
        #     #     else:
        #     #         self.reverse_asin[prod['product_asin']]['product_brand'] = prod['product_name'].split()[0]

        print(len(self.reverse_asin))
        asins = set()
        for d in self.list:
            for p in d["product_order"]:
                asins.add(p["product_asin"])
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
            # if self.reverse_asin[prod["product_asin"]]["product_brand"] in ["", "NA"]:
            #     if (
            #         "Blue Star"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Blue Star"
            #     elif (
            #         "Eureka Forbes"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Eureka Forbes"
            #     elif (
            #         "Hul Pureit"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Hul Pureit"
            #     elif "Kent" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Kent"
            #     elif (
            #         "Livpure" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Livpure"
            #     elif "Lloyd" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Lloyd"
            #     elif (
            #         "Voltas" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #         and "Voltas Beko"
            #         not in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Voltas"
            #     elif (
            #         "Panasonic"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Panasonic"
            #     elif "Haier" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Haier"
            #     elif "LG" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "LG"
            #     elif (
            #         "Daikin" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Daikin"
            #     elif (
            #         "Hitachi" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Hitachi"
            #     elif (
            #         "Samsung" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Samsung"
            #     elif (
            #         "Whirlpool"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Whirlpool"
            #     elif (
            #         "Liebherr"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Liebherr"
            #     elif (
            #         "Voltas Beko"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Voltas Beko"
            #     elif "Bosch" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Bosch"
            #     elif (
            #         "Havells" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Havells"
            #     elif (
            #         "Philips" in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "PHILIPS"
            #     elif "Syska" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "SYSKA"
            #     elif (
            #         "Crompton"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Crompton"
            #     elif (
            #         "Orient Electric"
            #         in self.reverse_asin[prod["product_asin"]]["product_name"]
            #     ):
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = "Orient Electric"
            #     elif "Bajaj" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Bajaj"
            #     elif "L'Oreal Paris" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "L'Oreal Paris"
            #     elif "Dove" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Dove"
            #     elif "L'Oréal Professionnel" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "L'Oréal Professionnel"
            #     elif "Biotique" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Biotique"
            #     elif "Head & Shoulders" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "Head & Shoulders"
            #     elif "TRESemme" in self.reverse_asin[prod["product_asin"]]["product_name"]:
            #         self.reverse_asin[prod["product_asin"]]["product_brand"] = "TRESemme"
            #     else:
            #         self.reverse_asin[prod["product_asin"]][
            #             "product_brand"
            #         ] = self.reverse_asin[prod["product_asin"]]["product_name"].split()[
            #             0
            #         ]
            # print('########')
            # print(prod["product_asin"])
            # print((self.reverse_asin[prod["product_asin"]]["product_name"]).encode("utf-8"))
            # print('########')
            try:
                i += 1
                present.add(prod["product_asin"])
                sos_rank_list.append(
                    {
                        "Keyword": doc["keyword"],
                        "Rank": i,  # prod['product_rank'],
                        "ASIN": prod["product_asin"],
                        "Title": self.reverse_asin[prod["product_asin"]][
                            "product_name"
                        ],
                        "Brand": " ".join(
                            self.reverse_asin[prod["product_asin"]]["product_brand"]
                            .lower()
                            .split()
                        ).title(),
                        "Latest Selling Price": self.reverse_asin[prod["product_asin"]][
                            "product_sale_price"
                        ],
                        "Latest Lowest Selling Price": (self.reverse_asin[prod["product_asin"]][
                            "product_sale_price"
                        ]).split("-")[0] if "-" in self.reverse_asin[prod["product_asin"]]["product_sale_price"] else self.reverse_asin[prod["product_asin"]]["product_sale_price"],
                        "Latest Highest Selling Price": (self.reverse_asin[prod["product_asin"]][
                            "product_sale_price"
                        ]).split("-")[1] if "-" in self.reverse_asin[prod["product_asin"]]["product_sale_price"] else self.reverse_asin[prod["product_asin"]]["product_sale_price"],
                        "Original Price": self.reverse_asin[prod["product_asin"]][
                            "product_original_price"
                        ],
                        "Lowest Original Price": (self.reverse_asin[prod["product_asin"]][
                            "product_original_price"
                        ]).split("-")[0] if "-" in self.reverse_asin[prod["product_asin"]]["product_original_price"] else self.reverse_asin[prod["product_asin"]]["product_original_price"],
                        "Highest Original Price": (self.reverse_asin[prod["product_asin"]][
                            "product_original_price"
                        ]).split("-")[1] if "-" in self.reverse_asin[prod["product_asin"]]["product_original_price"] else self.reverse_asin[prod["product_asin"]]["product_original_price"],
                        "Fulfilled": self.reverse_asin[prod["product_asin"]][
                            "product_fullfilled"
                        ],
                        "Sponsored": prod["sponsored"],
                        "Present": True,
                    }
                )
            except:
                print(prod["product_asin"])

        for asin, prod in self.reverse_asin.items():
            if asin not in present:
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
                        "Latest Lowest Selling Price": (prod["product_sale_price"]).split("-")[0] if "-" in prod["product_sale_price"] else prod["product_sale_price"],
                        "Latest Highest Selling Price": (prod["product_sale_price"]).split("-")[1] if "-" in prod["product_sale_price"] else prod["product_sale_price"],
                        "Original Price": prod["product_original_price"],
                        "Lowest Original Price": (prod["product_original_price"]).split("-")[0] if "-" in prod["product_original_price"] else prod["product_original_price"],
                        "Highest Original Price": (prod["product_original_price"]).split("-")[1] if "-" in prod["product_original_price"] else prod["product_original_price"],
                        "Fulfilled": prod["product_fullfilled"],
                        # 'Sponsored': prod['sponsored'],
                        "Present": False,
                    }
                )
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


if __name__ == "__main__":
    # cvt = Converter_sos('amazon_marketplace_scraping_havells_19_01_22/share_of_search.json', 'amazon_marketplace_scraping_havells_19_01_22/sos_data.json')
    # cvt.convert()
    # cvt.save('./amazon_sos.csv')

    cvt = Converter_sos(
        "../../data/InputData/biba/amazon_marketplace_scraping/share_of_search.json",
        "../../data/InputData/biba/amazon_marketplace_scraping/sos_data.json",
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/biba/amazon_marketplace_scraping/amazon_sos.csv"
    )

    # cvt = Converter_sos('../shell/amazon_marketplace_scraping_shell_25_02_22/share_of_search.json', '../shell/amazon_marketplace_scraping_shell_25_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../shell/amazon_sos.csv')

    # cvt = Converter_sos('../duracell/amazon_marketplace_scraping_duracell_16_02_22/share_of_search.json', '../duracell/amazon_marketplace_scraping_duracell_16_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../duracell/amazon_sos.csv')

    # cvt = Converter_sos('../hafele/amazon_marketplace_scraping_hafele_21_02_22/share_of_search.json', '../hafele/amazon_marketplace_scraping_hafele_21_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../hafele/amazon_sos.csv')

    # cvt = Converter_sos('../ferrero/amazon_marketplace_scraping_ferrero_28_02_22/share_of_search.json', '../ferrero/amazon_marketplace_scraping_ferrero_28_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ferrero/amazon_sos.csv')

    # cvt = Converter_sos('../kohler/amazon_marketplace_scraping_kohler_03_03_22/share_of_search.json', '../kohler/amazon_marketplace_scraping_kohler_03_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../kohler/amazon_sos.csv')

    # cvt = Converter_sos('../havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/share_of_search.json', '../havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../havells_fans/amazon_sos.csv')

    # cvt = Converter_sos('../havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/share_of_search.json', '../havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../havells_ac/amazon_sos.csv')

    # cvt = Converter_sos('../ghadi/amazon_marketplace_scraping_ghadi_05_03_22/share_of_search.json', '../ghadi/amazon_marketplace_scraping_ghadi_05_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ghadi/amazon_sos.csv')

    # cvt = Converter_sos('../nestle/amazon_marketplace_scraping_nestle_05_03_22/share_of_search.json', '../nestle/amazon_marketplace_scraping_nestle_05_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nestle/amazon_sos.csv')

    # cvt = Converter_sos('../park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/share_of_search.json', '../park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../park_avenue/amazon_sos.csv')

    # cvt = Converter_sos('../bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/share_of_search.json', '../bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../bajaj_mixer/amazon_sos.csv')

    # cvt = Converter_sos('../blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/share_of_search.json', '../blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../blue_heaven/amazon_sos.csv')

    # cvt = Converter_sos('../nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/share_of_search.json', '../nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nature_essence/amazon_sos.csv')

    # cvt = Converter_sos('../eno/amazon_marketplace_scraping_eno_12_04_22/share_of_search.json', '../eno/amazon_marketplace_scraping_eno_12_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../eno/amazon_sos.csv')

    # cvt = Converter_sos('../nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/share_of_search.json', '../nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nescafe_coffee_maker/amazon_sos.csv')

    # cvt = Converter_sos('../nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/share_of_search.json', '../nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nescafe_gold/amazon_sos.csv')

    # cvt = Converter_sos('../ceregrow/amazon_marketplace_scraping_ceregrow_13_04_22/share_of_search.json', '../ceregrow/amazon_marketplace_scraping_ceregrow_13_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ceregrow/amazon_sos.csv')

    # cvt = Converter_sos('../nestle_cornflakes/amazon_marketplace_scraping_nestle_cornflakes_15_05_22/share_of_search.json', '../nestle_cornflakes/amazon_marketplace_scraping_nestle_cornflakes_15_05_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nestle_cornflakes/amazon_sos.csv')

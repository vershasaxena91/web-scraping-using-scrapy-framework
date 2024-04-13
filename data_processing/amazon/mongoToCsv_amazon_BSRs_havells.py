from datetime import datetime
import json
import csv
from math import nan
import numpy as np
from numpy.lib.utils import info
import pandas as pd
from sqlalchemy import column
from tqdm import tqdm
import re
import pickle
from xgboost import XGBRegressor
from random import gauss, randint


class Converter:
    def __init__(self, filename):
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.prod_infos = pd.DataFrame(
            columns=["ASIN", "Category", "Latest BSR", "Last Day BSR"]
        )
        self.prod_moves = pd.DataFrame(
            columns=["ASIN", "Date", "Category", "BSR", "Last Known BSR"]
        )

    def convert_doc(self, i):
        def convert_string_to_day(sday):
            return datetime.strptime(sday, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y")

        def convert_string_to_bsr(s):
            sbsrs = s.split("#")
            if "" in sbsrs:
                sbsrs.remove("")
            cats = []
            for sbsr in sbsrs:
                e1 = re.search("[0-9,]+ in [^\(]+", sbsr)
                if e1 is None:
                    continue
                e1 = e1.group().split()
                val = e1[e1.index("in") - 1]
                cat = " ".join(e1[e1.index("in") + 1 :])
                # print(val, cat)
                cats.append({"category": cat, "value": int(val.replace(",", ""))})
            # print(cats)
            return cats

            # e1 = re.search("#[0-9,]+ in Beauty", sbsr)
            # if e1 is None:
            #     e1 = re.search("#[0-9,]+ in Health & Personal Care", sbsr)
            # e2 = re.search("#[0-9,]+", e1.group())
            # if e2 is None:
            #     return None
            # return int(e2.group().replace("#", "").replace(",", ""))

        # t = self.data[i]['product_best_seller_rank'][-1]['value'].split('#')
        # if '' in t:
        #     t.remove('')
        # return t
        self.data[i]["product_best_seller_rank"] = sorted(
            self.data[i]["product_best_seller_rank"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )

        prod_bsr_move = pd.DataFrame(
            columns=["ASIN", "Date", "Category", "BSR", "Last Known BSR"]
        )

        bsr_lists = dict()
        try:
            for b in self.data[i]["product_best_seller_rank"]:
                try:
                    bsr_cats = convert_string_to_bsr(b["value"])
                    # print(self.data[i]['product_asin'], len(bsr_cats))
                except:
                    bsr_cats = []
                # print(b['time'])
                b["time"] = convert_string_to_day(b["time"])
                # print(self.data[i]['product_asin'], len(bsr_cats))
                for c in bsr_cats:
                    bsr_lists[c["category"]] = bsr_lists.get(c["category"], [])
                    bsr_lists[c["category"]].append(
                        {
                            "Date": b["time"],
                            "ASIN": self.data[i]["product_asin"],
                            "BSR": c["value"],
                            "Category": c["category"],
                            # 'Last Known BSR': last_bsr[c['category']]['value']
                        }
                    )
                    # if (datetime.strptime(b['time'], "%d-%m-%Y") - datetime.strptime(last_bsr[c['category']]['day'], "%d-%m-%Y")).days > 0:
                    # last_bsr[c['category']]['day'] = b['time']
                    # last_bsr[c['category']]['value'] = c['value']
        except:
            pass
        prod_bsr_move_dict = dict()
        prod_bsr_info_list = []
        # print(bsr_lists.keys())
        for cat, bsrs in bsr_lists.items():
            prod_bsr_move_dict[cat] = pd.DataFrame(
                columns=["ASIN", "Date", "Category", "BSR", "Last Known BSR"]
            )
            prod_bsr_move_dict[cat] = prod_bsr_move_dict[cat].append(
                bsrs, ignore_index=True
            )
            # print(prod_bsr_move_dict[cat])
            prod_bsr_move_dict[cat] = pd.DataFrame(
                prod_bsr_move_dict[cat]
                .groupby(["ASIN", "Date", "Category"])["BSR"]
                .agg(lambda x: x.mean(skipna=False))
            )
            prod_bsr_move_dict[cat] = prod_bsr_move_dict[cat].reset_index()
            prod_bsr_move_dict[cat]["Date"] = pd.to_datetime(
                prod_bsr_move_dict[cat]["Date"], format="%d-%m-%Y"
            )
            prod_bsr_move_dict[cat] = prod_bsr_move_dict[cat].sort_values("Date")
            prod_bsr_move_dict[cat]["Date"] = prod_bsr_move_dict[cat][
                "Date"
            ].dt.strftime("%Y-%m-%d")
            prod_bsr_move_dict[cat] = prod_bsr_move_dict[cat].reset_index()
            prod_bsr_move_dict[cat] = prod_bsr_move_dict[cat].drop(columns="index")
            prod_bsr_move_dict[cat]["BSR f"] = np.nan
            for j in range(len(prod_bsr_move_dict[cat]["BSR"])):
                if pd.isna(prod_bsr_move_dict[cat].loc[j, "BSR"]):
                    try:
                        prod_bsr_move_dict[cat].loc[j, "BSR f"] = randint(
                            prod_bsr_move_dict[cat].loc[j - 15 : j - 1, "BSR"].min(),
                            prod_bsr_move_dict[cat].loc[j - 15 : j - 1, "BSR"].max(),
                        )
                    except:
                        prod_bsr_move_dict[cat].loc[j, "BSR f"] = np.nan

            prod_bsr_move_dict[cat]["BSR b"] = np.nan
            for j in range(len(prod_bsr_move_dict[cat]["BSR"]) - 1, -1, -1):
                if pd.isna(prod_bsr_move_dict[cat].loc[j, "BSR"]):
                    try:
                        prod_bsr_move_dict[cat].loc[j, "BSR b"] = randint(
                            prod_bsr_move_dict[cat].loc[j + 1 : j + 15, "BSR"].min(),
                            prod_bsr_move_dict[cat].loc[j + 1 : j + 15, "BSR"].max(),
                        )
                    except:
                        prod_bsr_move_dict[cat].loc[j, "BSR b"] = np.nan
                    fna = pd.isna(prod_bsr_move_dict[cat].loc[j, "BSR f"])
                    bna = pd.isna(prod_bsr_move_dict[cat].loc[j, "BSR b"])
                    if not (fna or bna):
                        prod_bsr_move_dict[cat].loc[j, "BSR"] = (
                            prod_bsr_move_dict[cat].loc[j, "BSR f"]
                            + prod_bsr_move_dict[cat].loc[j, "BSR b"]
                        ) / 2
                    elif not fna:
                        prod_bsr_move_dict[cat].loc[j, "BSR"] = prod_bsr_move_dict[
                            cat
                        ].loc[j, "BSR f"]
                    elif not bna:
                        prod_bsr_move_dict[cat].loc[j, "BSR"] = prod_bsr_move_dict[
                            cat
                        ].loc[j, "BSR b"]
            prod_bsr_move_dict[cat].drop(columns=["BSR f", "BSR b"], inplace=True)
            try:
                prod_bsr_move_dict[cat]["BSR"] = prod_bsr_move_dict[cat]["BSR"].round()
            except:
                pass
            prod_bsr_move_dict[cat]["Last Known BSR"] = prod_bsr_move_dict[cat][
                "BSR"
            ].shift(periods=1)
            # print("Cat: ")
            # print(cat)
            # print(len(prod_bsr_move_dict[cat]))
            # print(prod_bsr_move_dict[cat])
            # print(prod_bsr_move_dict[cat]['BSR'].iloc[-1])
            prod_bsr_info_list.append(
                {
                    "ASIN": self.data[i]["product_asin"],
                    "Latest BSR": prod_bsr_move_dict[cat]["BSR"].iloc[-1]
                    if len(prod_bsr_move_dict[cat]) > 0
                    else None,
                    "Last Day BSR": prod_bsr_move_dict[cat]["Last Known BSR"].iloc[-1]
                    if len(prod_bsr_move_dict[cat]) > 0
                    else None,
                    "Category": cat,
                }
            )
            prod_bsr_move = prod_bsr_move.append(prod_bsr_move_dict[cat])
        if len(prod_bsr_info_list) == 0:
            return None, None
        prod_bsr_info = pd.DataFrame(prod_bsr_info_list)
        # print(prod_bsr_info)
        return prod_bsr_info, prod_bsr_move

    def convert(self):
        #     mx = 0
        #     bsr = None
        failed = []
        for i in tqdm(range(len(self.data))):
            # t = self.convert_doc(i)
            # if len(t) > mx:
            #     mx = len(t)
            #     bsr = t
            prod_info, prod_move = self.convert_doc(i)
            if prod_info is not None and prod_move is not None:
                self.prod_infos = self.prod_infos.append(prod_info, ignore_index=True)
                self.prod_moves = self.prod_moves.append(prod_move, ignore_index=True)
                # if i > 5:
                #     break
            else:
                # print(self.data[i]['product_asin'])
                failed.append(
                    {
                        "ASIN": self.data[i]["product_asin"],
                        "Name": self.data[i]["product_name"],
                    }
                )
        # print(mx)
        # print(bsr)
        print(failed)
        print(len(failed))
        f = pd.DataFrame(failed)
        f.to_csv("../../data/OutputData/manyavar/no_bsrs.csv", index=False)

    def save(self, info_file, move_file):
        self.prod_infos.to_csv(info_file, index=False)
        self.prod_moves.to_csv(move_file, index=False)


if __name__ == "__main__":
    # cvt = Converter('amazon_marketplace_scraping_havells_11_02_22/product_data.json')
    # cvt.convert()
    # cvt.save("amazon_bsr_info.csv", "amazon_bsr_move.csv")

    cvt = Converter(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_data.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_bsr_info.csv",
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_bsr_move.csv",
    )

    # cvt = Converter('../shell/amazon_marketplace_scraping_shell_01_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../shell/amazon_bsr_info.csv", "../shell/amazon_bsr_move.csv")

    # cvt = Converter('../duracell/amazon_marketplace_scraping_duracell_02_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../duracell/amazon_bsr_info.csv", "../duracell/amazon_bsr_move.csv")

    # cvt = Converter('../hafele/amazon_marketplace_scraping_hafele_21_02_22/product_data.json')
    # cvt.convert()
    # cvt.save("../hafele/amazon_bsr_info.csv", "../hafele/amazon_bsr_move.csv")

    # cvt = Converter('../ferrero/amazon_marketplace_scraping_ferrero_02_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../ferrero/amazon_bsr_info.csv", "../ferrero/amazon_bsr_move.csv")

    # cvt = Converter('../kohler/amazon_marketplace_scraping_kohler_03_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../kohler/amazon_bsr_info.csv", "../kohler/amazon_bsr_move.csv")

    # cvt = Converter('../havells_fans/amazon_marketplace_scraping_havells_fans_31_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../havells_fans/amazon_bsr_info.csv", "../havells_fans/amazon_bsr_move.csv")

    # cvt = Converter('../havells_ac/amazon_marketplace_scraping_havells_ac_31_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../havells_ac/amazon_bsr_info.csv", "../havells_ac/amazon_bsr_move.csv")

    # cvt = Converter('../ghadi/amazon_marketplace_scraping_ghadi_05_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../ghadi/amazon_bsr_info.csv", "../ghadi/amazon_bsr_move.csv")

    # cvt = Converter('../nestle/amazon_marketplace_scraping_nestle_06_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../nestle/amazon_bsr_info.csv", "../nestle/amazon_bsr_move.csv")

    # cvt = Converter('../park_avenue/amazon_marketplace_scraping_park_avenue_21_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../park_avenue/amazon_bsr_info.csv", "../park_avenue/amazon_bsr_move.csv")

    # cvt = Converter('../bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_27_03_22/product_data.json')
    # cvt.convert()
    # cvt.save("../bajaj_mixer/amazon_bsr_info.csv", "../bajaj_mixer/amazon_bsr_move.csv")

    # cvt = Converter('../blue_heaven/amazon_marketplace_scraping_blue_heaven_06_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../blue_heaven/amazon_bsr_info.csv", "../blue_heaven/amazon_bsr_move.csv")

    # cvt = Converter('../nature_essence/amazon_marketplace_scraping_nature_essence_06_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../nature_essence/amazon_bsr_info.csv", "../nature_essence/amazon_bsr_move.csv")

    # cvt = Converter('../eno/amazon_marketplace_scraping_eno_12_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../eno/amazon_bsr_info.csv", "../eno/amazon_bsr_move.csv")

    # cvt = Converter('../nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../nescafe_coffee_maker/amazon_bsr_info.csv", "../nescafe_coffee_maker/amazon_bsr_move.csv")

    # cvt = Converter('../nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../nescafe_gold/amazon_bsr_info.csv", "../nescafe_gold/amazon_bsr_move.csv")

    # cvt = Converter('../ceregrow/amazon_marketplace_scraping_ceregrow_18_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../ceregrow/amazon_bsr_info.csv", "../ceregrow/amazon_bsr_move.csv")

    # cvt = Converter('../nestle_cornflakes/amazon_marketplace_scraping_nestle_cornflakes_16_05_22/product_data.json')
    # cvt.convert()
    # cvt.save("../nestle_cornflakes/amazon_bsr_info.csv", "../nestle_cornflakes/amazon_bsr_move.csv")

    # cvt = Converter('../kitkat/amazon_marketplace_scraping_kitkat_18_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../kitkat/amazon_bsr_info.csv", "../kitkat/amazon_bsr_move.csv")

    # cvt = Converter('../maggi/amazon_marketplace_scraping_maggi_18_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../maggi/amazon_bsr_info.csv", "../maggi/amazon_bsr_move.csv")

    # cvt = Converter('../nescafe_sunrise/amazon_marketplace_scraping_nescafe_sunrise_18_04_22/product_data.json')
    # cvt.convert()
    # cvt.save("../nescafe_sunrise/amazon_bsr_info.csv", "../nescafe_sunrise/amazon_bsr_move.csv")

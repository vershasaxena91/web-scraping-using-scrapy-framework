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

            cats = []

            cats.append({"category": "Shampoo", "value": int(s)})
            return cats

        self.data[i]["product_rank"] = self.data[i].get("product_rank", [])
        self.data[i]["product_rank"] = sorted(
            self.data[i]["product_rank"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )

        prod_bsr_move = pd.DataFrame(
            columns=["ASIN", "Date", "Category", "BSR", "Last Known BSR"]
        )

        bsr_lists = dict()
        try:
            for b in self.data[i]["product_rank"]:
                try:
                    bsr_cats = convert_string_to_bsr(b["value"])

                except:
                    bsr_cats = []
                b["time"] = convert_string_to_day(b["time"])

                for c in bsr_cats:
                    bsr_lists[c["category"]] = bsr_lists.get(c["category"], [])
                    bsr_lists[c["category"]].append(
                        {
                            "Date": b["time"],
                            "ASIN": self.data[i]["product_id"],
                            "BSR": c["value"],
                            "Category": c["category"],
                        }
                    )

        except:
            pass
        prod_bsr_move_dict = dict()
        prod_bsr_info_list = []
        for cat, bsrs in bsr_lists.items():
            prod_bsr_move_dict[cat] = pd.DataFrame(
                columns=["ASIN", "Date", "Category", "BSR", "Last Known BSR"]
            )
            prod_bsr_move_dict[cat] = prod_bsr_move_dict[cat].append(
                bsrs, ignore_index=True
            )

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

            prod_bsr_info_list.append(
                {
                    "ASIN": self.data[i]["product_id"],
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

        return prod_bsr_info, prod_bsr_move

    def convert(self):

        failed = []
        for i in tqdm(range(len(self.data))):

            prod_info, prod_move = self.convert_doc(i)
            if prod_info is not None and prod_move is not None:
                self.prod_infos = self.prod_infos.append(prod_info, ignore_index=True)
                self.prod_moves = self.prod_moves.append(prod_move, ignore_index=True)

            else:

                failed.append(
                    {
                        "ASIN": self.data[i]["product_id"],
                        "Name": self.data[i]["product_name"],
                    }
                )

        print(failed)
        print(len(failed))
        f = pd.DataFrame(failed)
        f.to_csv("../../data/OutputData/loreal_shampoo/no_bsrs.csv", index=False)

    def save(self, info_file, move_file):
        self.prod_infos.to_csv(info_file, index=False)
        self.prod_moves.to_csv(move_file, index=False)


if __name__ == "__main__":

    cvt = Converter(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/product_data.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_bsr_info.csv",
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_bsr_move.csv",
    )

from datetime import datetime
import json
import csv
from math import nan
import numpy as np
from numpy.lib.utils import info
import pandas as pd
from tqdm import tqdm
import re
import pickle
from xgboost import XGBRegressor
from random import gauss, randint


class Converter:
    def __init__(self, filename):
        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.prod_infos = pd.DataFrame(
            columns=[
                "ASIN",
                "Title",
                "Brand",
                "Latest Selling Price",
                "Latest BSR",
                "Reviews",
                "Ratings",
                "Sentiment Score",
                "Details Changed",
            ]
        )
        self.prod_moves = pd.DataFrame(
            columns=[
                "ASIN",
                "Date",
                "Selling Price",
                "Last Known Selling Price",
                "BSR",
                "Last Known BSR",
            ]
        )

    def convert_doc(self, i):
        def convert_string_to_day(sday):
            return datetime.strptime(sday, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y")

        self.data[i]["product_sale_price"] = self.data[i].get("product_sale_price", [])
        self.data[i]["product_sale_price"] = sorted(
            self.data[i]["product_sale_price"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_rank"] = self.data[i].get("product_rank", [])
        self.data[i]["product_rank"] = sorted(
            self.data[i]["product_rank"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_availability"] = self.data[i].get(
            "product_availability", []
        )
        self.data[i]["product_availability"] = sorted(
            self.data[i]["product_availability"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_rating"] = self.data[i].get("product_rating", [])
        self.data[i]["product_rating"] = sorted(
            self.data[i]["product_rating"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_total_reviews"] = self.data[i].get(
            "product_total_reviews", []
        )
        self.data[i]["product_total_reviews"] = sorted(
            self.data[i]["product_total_reviews"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )

        prod_price_move = pd.DataFrame(
            columns=["ASIN", "Date", "Selling Price", "Last Known Selling Price"]
        )
        prod_bsr_move = pd.DataFrame(columns=["ASIN", "Date", "BSR", "Last Known BSR"])
        prod_avail_move = pd.DataFrame(columns=["ASIN", "Date", "Availability"])

        price = None
        last_price = None
        price_list = []
        last_day = None
        day = None
        try:
            for p in self.data[i]["product_sale_price"]:
                try:
                    p["value"] = float(p["value"].replace(",", ""))
                except:
                    p["value"] = np.nan
                p["time"] = convert_string_to_day(p["time"])

                price_list.append(
                    {
                        "Date": p["time"],
                        "ASIN": self.data[i]["product_id"],
                        "Selling Price": p["value"],
                    }
                )
                if p["value"] is not None:
                    if (
                        last_day is None
                        or (
                            datetime.strptime(p["time"], "%d-%m-%Y")
                            - datetime.strptime(last_day, "%d-%m-%Y")
                        ).days
                        > 0
                    ):
                        last_price = price
                        last_day = day
                    day = p["time"]
                    price = p["value"]
        except:
            pass
        if len(price_list) > 0:
            prod_price_move = prod_price_move.append(price_list, ignore_index=True)
            prod_price_move = pd.DataFrame(
                prod_price_move.groupby(["ASIN", "Date"])["Selling Price"].agg(
                    lambda x: x.mean(skipna=False)
                )
            )
            prod_price_move = prod_price_move.reset_index()

            prod_price_move["Date"] = pd.to_datetime(
                prod_price_move["Date"], format="%d-%m-%Y"
            )
            prod_price_move = prod_price_move.sort_values("Date")
            prod_price_move["Date"] = prod_price_move["Date"].dt.strftime("%Y-%m-%d")
            prod_price_move = prod_price_move.reset_index()
            prod_price_move = prod_price_move.drop(columns="index")

        bsr = None
        last_bsr = None
        bsr_list = []
        last_day = None
        day = None
        try:
            for b in self.data[i]["product_rank"]:
                try:
                    b["value"] = float(b["value"])
                except:
                    b["value"] = np.nan
                b["time"] = convert_string_to_day(b["time"])

                bsr_list.append(
                    {
                        "Date": b["time"],
                        "ASIN": self.data[i]["product_id"],
                        "BSR": b["value"],
                    }
                )
                if b["value"] is not None and not np.isnan(b["value"]):
                    if (
                        last_day is None
                        or (
                            datetime.strptime(b["time"], "%d-%m-%Y")
                            - datetime.strptime(last_day, "%d-%m-%Y")
                        ).days
                        > 0
                    ):
                        last_bsr = bsr
                        last_day = day
                    day = b["time"]
                    bsr = b["value"]
        except:
            pass
        if len(bsr_list) > 0:
            prod_bsr_move = prod_bsr_move.append(bsr_list, ignore_index=True)
            prod_bsr_move = pd.DataFrame(
                prod_bsr_move.groupby(["ASIN", "Date"])["BSR"].agg(
                    lambda x: x.mean(skipna=False)
                )
            )
            prod_bsr_move = prod_bsr_move.reset_index()
            prod_bsr_move["Date"] = pd.to_datetime(
                prod_bsr_move["Date"], format="%d-%m-%Y"
            )
            prod_bsr_move = prod_bsr_move.sort_values("Date")
            prod_bsr_move["Date"] = prod_bsr_move["Date"].dt.strftime("%Y-%m-%d")
            prod_bsr_move = prod_bsr_move.reset_index()
            prod_bsr_move = prod_bsr_move.drop(columns="index")

        try:
            # Avail Loop
            avail_list = []
            for a in self.data[i]["product_availability"]:
                try:
                    a["value"] = False if a["value"] == "false" else True
                except:
                    a["value"] = False
                a["time"] = convert_string_to_day(a["time"])
                avail_list.append(
                    {
                        "ASIN": self.data[i]["product_id"],
                        "Date": a["time"],
                        "Availability": a["value"],
                    }
                )
            prod_avail_move = prod_avail_move.append(avail_list, ignore_index=True)
            prod_avail_move = pd.DataFrame(
                prod_avail_move.groupby(["ASIN", "Date"])["Availability"].agg(
                    lambda x: x.mean(skipna=False)
                )
            )
            prod_avail_move = prod_avail_move.reset_index()

            prod_avail_move["Date"] = pd.to_datetime(
                prod_avail_move["Date"], format="%d-%m-%Y"
            )
            prod_avail_move = prod_avail_move.sort_values("Date")
            prod_avail_move["Date"] = prod_avail_move["Date"].dt.strftime("%Y-%m-%d")
            prod_avail_move = prod_avail_move.reset_index()
            prod_avail_move = prod_avail_move.drop(columns="index")
            avail_list.clear()
        except:
            pass
        avail_days = set()
        unavail_days = set()
        print(self.data[i]["product_name"])
        try:
            last_day = self.data[i]["product_availability"][-1]["time"]

            for a in self.data[i]["product_availability"][::-1]:
                day = a["time"]
                assert datetime.strptime(day, "%d-%m-%Y") <= datetime.strptime(
                    last_day, "%d-%m-%Y"
                )
                if (
                    datetime.strptime(last_day, "%d-%m-%Y")
                    - datetime.strptime(day, "%d-%m-%Y")
                ).days > 15:
                    break
                if a["value"]:
                    avail_days.add(day)
                else:
                    unavail_days.add(day)
            print(len(unavail_days))
            print(len(avail_days))
        except:
            pass

        prod_move = prod_price_move.merge(
            prod_bsr_move, how="inner", on=["ASIN", "Date"]
        )

        prod_move["Selling Price f"] = np.nan
        for j in range(len(prod_move["Selling Price"])):
            if pd.isna(prod_move.loc[j, "Selling Price"]):
                try:
                    prod_move.loc[j, "Selling Price f"] = randint(
                        prod_move.loc[j - 15 : j - 1, "Selling Price"].min(),
                        prod_move.loc[j - 15 : j - 1, "Selling Price"].max(),
                    )
                except:
                    prod_move.loc[j, "Selling Price f"] = np.nan

        prod_move["Selling Price b"] = np.nan
        for j in range(len(prod_move["Selling Price"]) - 1, -1, -1):
            if pd.isna(prod_move.loc[j, "Selling Price"]):
                try:
                    prod_move.loc[j, "Selling Price b"] = randint(
                        prod_move.loc[j + 1 : j + 15, "Selling Price"].min(),
                        prod_move.loc[j + 1 : j + 15, "Selling Price"].max(),
                    )
                except:
                    prod_move.loc[j, "Selling Price b"] = np.nan
                fna = pd.isna(prod_move.loc[j, "Selling Price f"])
                bna = pd.isna(prod_move.loc[j, "Selling Price b"])
                if not (fna or bna):
                    prod_move.loc[j, "Selling Price"] = (
                        prod_move.loc[j, "Selling Price f"]
                        + prod_move.loc[j, "Selling Price b"]
                    ) / 2
                elif not fna:
                    prod_move.loc[j, "Selling Price"] = prod_move.loc[
                        j, "Selling Price f"
                    ]
                elif not bna:
                    prod_move.loc[j, "Selling Price"] = prod_move.loc[
                        j, "Selling Price b"
                    ]
        prod_move.drop(columns=["Selling Price f", "Selling Price b"], inplace=True)
        try:
            prod_move["Selling Price"] = prod_move["Selling Price"].round()
        except:
            pass

        prod_move["BSR f"] = np.nan
        for j in range(len(prod_move["BSR"])):
            if pd.isna(prod_move.loc[j, "BSR"]):
                try:
                    prod_move.loc[j, "BSR f"] = randint(
                        prod_move.loc[j - 15 : j - 1, "BSR"].min(),
                        prod_move.loc[j - 15 : j - 1, "BSR"].max(),
                    )
                except:
                    prod_move.loc[j, "BSR f"] = np.nan

        prod_move["BSR b"] = np.nan
        for j in range(len(prod_move["BSR"]) - 1, -1, -1):
            if pd.isna(prod_move.loc[j, "BSR"]):
                try:
                    prod_move.loc[j, "BSR b"] = randint(
                        prod_move.loc[j + 1 : j + 15, "BSR"].min(),
                        prod_move.loc[j + 1 : j + 15, "BSR"].max(),
                    )
                except:
                    prod_move.loc[j, "BSR b"] = np.nan
                fna = pd.isna(prod_move.loc[j, "BSR f"])
                bna = pd.isna(prod_move.loc[j, "BSR b"])
                if not (fna or bna):
                    prod_move.loc[j, "BSR"] = (
                        prod_move.loc[j, "BSR f"] + prod_move.loc[j, "BSR b"]
                    ) / 2
                elif not fna:
                    prod_move.loc[j, "BSR"] = prod_move.loc[j, "BSR f"]
                elif not bna:
                    prod_move.loc[j, "BSR"] = prod_move.loc[j, "BSR b"]
        prod_move.drop(columns=["BSR f", "BSR b"], inplace=True)
        try:
            prod_move["BSR"] = prod_move["BSR"].round()
        except:
            pass

        prod_move["Last Known Selling Price"] = prod_move["Selling Price"].shift(
            periods=1
        )
        prod_move["Last Known BSR"] = prod_move["BSR"].shift(periods=1)
        prod_move["Estimated Sales"] = 0

        if not isinstance(self.data[i]["product_brand"], str) or self.data[i][
            "product_brand"
        ] in ["", "NA"]:
            if "Dove" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Dove"
            elif "L'Oreal Professionnel" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "L'Oreal Professionnel"
            elif "L'Oreal Paris" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "L'Oreal Paris"
            elif "Biotique" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Biotique"
            elif "Tresemme" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Tresemme"
            elif "Head & Shoulders" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Head & Shoulders"
            else:
                self.data[i]["product_brand"] = self.data[i]["product_name"].split()[0]

        try:
            reviews = int(
                self.data[i]["product_total_reviews"][-1]["value"]
                .split()[0]
                .replace(",", "")
            )
        except:
            reviews = None
        try:
            ratings = float(
                self.data[i]["product_rating"][-1]["value"].split(" out ")[0]
            )
        except:
            ratings = None
        try:
            self.data[i]["product_original_price"] = float(
                self.data[i]["product_original_price"][-1]["value"]
            )
        except:
            self.data[i]["product_original_price"] = None

        if price is None and bsr is None:
            return None, None

        prod_move = prod_move.merge(prod_avail_move, how="inner", on=["ASIN", "Date"])
        category = ""
        if "Shampoo" in self.data[i].get("product_category", []):
            category = "Shampoo"
        else:
            category = ([None] + self.data[i].get("product_category", []))[-1]

        gender = ""
        prod_info = pd.DataFrame(
            [
                {
                    "ASIN": self.data[i]["product_id"],
                    "Title": self.data[i]["product_name"],
                    "Brand": " ".join(
                        self.data[i]["product_brand"].lower().split()
                    ).title(),
                    "Original Price": self.data[i]["product_original_price"],
                    "Latest Selling Price": price,
                    "Last Day Selling Price": last_price,
                    "Latest BSR": bsr,
                    "Last Day BSR": last_bsr,
                    "Estimated Sales": 0,
                    "Availability": len(avail_days)
                    / len(avail_days.union(unavail_days))
                    if len(avail_days) > 0
                    else 0,
                    "Reviews": reviews,
                    "Ratings": ratings,
                    "Sentiment Score": "NA",
                    "Details Changed": "NA",
                    "Category": category,
                    "Gender": gender,
                }
            ]
        )

        return prod_info, prod_move

    def convert(self):

        for i in tqdm(range(len(self.data))):
            prod_info, prod_move = self.convert_doc(i)

            if prod_info is not None and prod_move is not None:
                self.prod_infos = self.prod_infos.append(prod_info, ignore_index=True)
                self.prod_moves = self.prod_moves.append(prod_move, ignore_index=True)

    def save(self, info_file, move_file):
        self.prod_infos.to_csv(info_file, index=False)
        self.prod_moves.to_csv(move_file, index=False)


if __name__ == "__main__":

    cvt = Converter(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/product_data.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_prod_info.csv",
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_prod_move.csv",
    )

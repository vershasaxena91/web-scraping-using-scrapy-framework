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
    def __init__(self, filename):  # , model):
        # with open(filename, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())
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
        # self.estimation_model = XGBRegressor()
        # self.estimation_model.load_model(model)

    def convert_doc(self, i):
        def convert_string_to_day(sday):
            return datetime.strptime(sday, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y")

        def convert_string_to_bsr(sbsr):
            # VALUE(
            #     TO_PURE_NUMBER(
            #         REGEXREPLACE(
            #             REGEXREPLACE(
            #                 REGEXEXTRACT(
            #                     REGEXEXTRACT(
            #                         $D2,
            #                         "#[0-9,]+ in Shampoos"
            #                     ),
            #                     "#[0-9,]+"
            #                 ),
            #                 "#", ""
            #             ),
            #             ",", ""
            #         )
            #     )
            # )
            e1 = re.search("#[0-9,]+ in Beauty", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Health & Personal Care", sbsr)
            e2 = re.search("#[0-9,]+", e1.group())
            if e2 is None:
                return None
            return int(e2.group().replace("#", "").replace(",", ""))

        # estimation_model = pickle.load(open('XGBoost_Model.sav', 'rb'))

        def estimate_sales(price, bsr):
            # print(bsr, type(bsr), np.isnan(bsr))
            try:
                if bsr is None or np.isnan(bsr):
                    return None
                elif bsr == 1:
                    return 110
                elif bsr == 2:
                    return 106
                elif bsr == 3:
                    return 102
                elif bsr == 4:
                    return 99
                elif bsr == 5:
                    return 96
                elif bsr == 6:
                    return 93
                elif bsr == 7:
                    return 90
                elif bsr == 8:
                    return 88
                elif bsr == 9:
                    return 86
                elif bsr == 10:
                    return 84
                elif bsr == 11:
                    return 82
            except:
                print(bsr)
            # print(price, bsr)
            sales = self.estimation_model.predict(np.array([[price, bsr]]))
            # print(sales)
            return max(round(sales[0]), 0)

        def estimate_sales_with_df(price_bsr_df):
            return self.estimation_model.predict(price_bsr_df).round()

        self.data[i]["product_rank"] = self.data[i].get("product_rank", [])
        print(self.data[i]["product_rank"])
        self.data[i]["product_availability"] = [
            rw if rw is not None else {"time": "2022-01-25 23:56:42", "value": False}
            for rw in self.data[i]["product_availability"]
        ]
        self.data[i]["product_total_reviews"] = [
            rw for rw in self.data[i]["product_total_reviews"] if rw is not None
        ]

        self.data[i]["product_sale_price"] = sorted(
            self.data[i]["product_sale_price"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_rank"] = sorted(
            self.data[i]["product_rank"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_availability"] = sorted(
            self.data[i]["product_availability"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_assured"] = sorted(
            self.data[i]["product_assured"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_rating"] = sorted(
            self.data[i]["product_rating"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
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
                # To remove
                # print(p['time'])
                # if (datetime.strptime(p['time'], "%d-%m-%Y") - datetime.strptime("26-11-2021", "%d-%m-%Y")).days <= 0:
                # print("Added")
                price_list.append(
                    {
                        "Date": p["time"],
                        "ASIN": self.data[i]["product_pid"],
                        "Selling Price": p["value"],
                        # 'Last Known Selling Price': price
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
        prod_price_move = prod_price_move.append(
            price_list, ignore_index=True, sort=False
        )
        # print(prod_price_move)
        prod_price_move = pd.DataFrame(
            prod_price_move.groupby(["ASIN", "Date"])["Selling Price"].agg(
                lambda x: x.mean(skipna=False)
            )
        )
        # print(prod_price_move)
        prod_price_move = prod_price_move.reset_index()
        prod_price_move["Date"] = pd.to_datetime(
            prod_price_move["Date"], format="%d-%m-%Y"
        )
        # print(prod_price_move['Date'])
        prod_price_move = prod_price_move.sort_values("Date")
        # print(prod_price_move['Date'])
        prod_price_move["Date"] = prod_price_move["Date"].dt.strftime("%Y-%m-%d")
        prod_price_move = prod_price_move.reset_index()
        prod_price_move = prod_price_move.drop(columns="index")
        print(prod_price_move)
        # print(type(prod_price_move))
        # exit()
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
                # print(b['time'])
                # To remove
                # if (datetime.strptime(b['time'], "%d-%m-%Y") - datetime.strptime("26-11-2021", "%d-%m-%Y")).days <= 0:
                # print("Added", b['value'])
                bsr_list.append(
                    {
                        "Date": b["time"],
                        "ASIN": self.data[i]["product_pid"],
                        "BSR": b["value"],
                        # 'Last Known BSR': bsr
                    }
                )
                if b["value"] is not None:
                    # print(b)
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
            print(prod_bsr_move)
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

        assured = None
        try:
            for f in self.data[i]["product_assured"]:
                if f["value"] is not None:
                    assured = f["value"]
        except:
            pass

        try:
            # Avail Loop
            avail_list = []
            for a in self.data[i]["product_availability"]:
                try:
                    a["value"] = False if a["value"] in ["Sold Out"] else True
                except:
                    a["value"] = False
                a["time"] = convert_string_to_day(a["time"])
                avail_list.append(
                    {
                        "ASIN": self.data[i]["product_pid"],
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
            # last_day = convert_string_to_day(self.data[i]['product_availability'][-1]['time'])
            last_day = self.data[i]["product_availability"][-1]["time"]
            # To remove
            # last_day = "24-12-2021"
            for a in self.data[i]["product_availability"][::-1]:
                # day = convert_string_to_day(a['time'])
                day = a["time"]
                # To remove
                # print(day)
                # if (datetime.strptime(day, "%d-%m-%Y") - datetime.strptime("26-11-2021", "%d-%m-%Y")).days <= 0:
                # print("Added")
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

        # print("PRICE:", prod_price_move)
        # print("BSR:", prod_bsr_move)
        # prod_move = pd.DataFrame(columns=['ASIN', 'Date', 'Selling Price', 'Last Known Selling Price', 'BSR', 'Last Known BSR', 'Availability'])
        prod_move = prod_price_move.merge(
            prod_bsr_move, how="outer", on=["ASIN", "Date"]
        )

        # print(prod_move.info())
        # mean_price = prod_move['Selling Price'].mean()
        # std_price = prod_move['Selling Price'].std()
        # mean_bsr = prod_move['BSR'].mean()
        # std_bsr = prod_move['BSR'].std()
        # if pd.isna(mean_price):
        #     mean_price = 0
        # if pd.isna(std_price):
        #     std_price = 0
        # if pd.isna(mean_bsr):
        #     mean_bsr = 0
        # if pd.isna(std_bsr):
        #     std_bsr = 0
        # min_price = round(prod_move['Selling Price'].min())
        # max_price = round(prod_move['Selling Price'].max())
        # min_bsr = round(prod_move['BSR'].min())
        # max_bsr = round(prod_move['BSR'].max())
        # # print(mean_price, std_price)
        # # print(mean_bsr, std_bsr)
        # # prod_move['Selling Price'] = prod_move['Selling Price'].apply(lambda x: float(round(gauss(mean_price, std_price))) if pd.isna(x) else x)
        # # prod_move['BSR'] = prod_move['BSR'].apply(lambda x: float(round(gauss(mean_bsr, std_bsr))) if pd.isna(x) else x)
        # # print(min_price, max_price)
        # # print(min_bsr, max_bsr)
        # prod_move['Selling Price'] = prod_move['Selling Price'].apply(lambda x: randint(min_price, max_price) if pd.isna(x) else x)
        # prod_move['BSR'] = prod_move['BSR'].apply(lambda x: randint(min_bsr, max_bsr) if pd.isna(x) else x)

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
        # prod_move["Estimated Sales"] = prod_move[["Selling Price", "BSR"]].apply(
        #     lambda x: estimate_sales(x[0], x[1])
        # )
        # prod_move["Estimated Sales"] = list(
        #     map(estimate_sales, prod_move["Selling Price"], prod_move["BSR"])
        # )
        # prod_move["Estimated Sales"] = prod_move.apply(
        #     lambda x: estimate_sales(x["Selling Price"], x["BSR"]), axis=1
        # )
        # try:
        #     prod_move['Estimated Sales'] = estimate_sales_with_df(prod_move[['Selling Price', 'BSR']])
        #     for j in range(len(prod_move)):
        #         prod_move.loc[j, 'Estimated Sales'] = estimate_sales(prod_move.loc[j, 'Selling Price'], prod_move.loc[j, 'BSR'])
        #         prod_move['Estimated Sales'] = prod_move.apply(lambda x: "NA" if x['Selling Price'] is None or x['BSR'] is None else x['Estimated Sales'])
        # except:
        #     prod_move['Estimated Sales'] = "NA"

        # if not isinstance(self.data[i]['product_brand'], str) or self.data[i]['product_brand'] in ["", "NA"]:
        #     if "L'Oreal Paris" in self.data[i]['product_name'] or "Loreal" in self.data[i]['product_name'] or "L'Oreal" in self.data[i]['product_name'] or "L'oreal Paris" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("L'Oreal Paris")
        #     elif "Head & Shoulders" in self.data[i]['product_name'] or "Head and Shoulders" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Head & Shoulders")
        #     elif "BIOTIQUE" in self.data[i]['product_name'] or "Biotique" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Biotique")
        #     elif "Tresemme" in self.data[i]['product_name'] or "TRESemme" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("TRESemme")
        #     elif "Dove" in self.data[i]['product_name'] or "DOVE" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Dove")
        #     elif "MATRIX" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Matrix")
        #     elif "BIOLAGE" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Biolage")
        #     elif "Pantene" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Pantene")
        #     elif "Indulekha" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Indulekha")
        #     elif "Nyle" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Nyle")
        #     elif "Sunsilk" in self.data[i]['product_name']:
        #         self.data[i]['product_brand'] = ("Sunsilk")
        #     else:
        #         self.data[i]['product_brand'] = ("Others")

        if not isinstance(self.data[i]["product_brand"], str) or self.data[i][
            "product_brand"
        ] in ["", "NA"]:
            self.data[i]["product_brand"] = self.data[i]["product_name"].split()[0]

        # print(self.data[i]['product_total_reviews'][-1]['value'].split()[0].replace(',', ''))
        try:
            total_reviews = int(
                self.data[i]["product_total_reviews"][-1]["value"]
                .split()[0]
                .replace(",", "")
            )
        except:
            total_reviews = None
        try:
            ratings = float(
                self.data[i]["product_rating"][-1]["value"].split(" out ")[0]
            )
        except:
            ratings = None
        try:
            total_ratings = int(
                self.data[i]["product_total_ratings"][-1]["value"]
                .split()[0]
                .replace(",", "")
            )
        except:
            total_ratings = None
        try:
            self.data[i]["product_original_price"] = float(
                self.data[i]["product_original_price"]
            )
        except:
            self.data[i]["product_original_price"] = None

        if price is None and bsr is None:
            return None, None
        # print(i, self.data[i]['product_pid'])

        prod_move = prod_move.merge(prod_avail_move, how="outer", on=["ASIN", "Date"])
        category = ""
        if "Hair Dryers & Accessories" in self.data[i].get("product_category", []):
            category = "Hair Dryers & Accessories"
        elif "Straighteners" in self.data[i].get("product_category", []):
            category = "Straighteners"
        elif "Curling Irons" in self.data[i].get("product_category", []):
            category = "Curling Irons"
        elif "Shaving, Waxing & Beard Care" in self.data[i].get("product_category", []):
            category = "Shaving, Waxing & Beard Care"
        elif "Electric Massagers" in self.data[i].get("product_category", []):
            category = "Electric Massagers"
        elif "Foot Care" in self.data[i].get("product_category", []):
            category = "Foot Care"
        elif "Skin Care Tools" in self.data[i].get("product_category", []):
            category = "Skin Care Tools"
        elif "Hair Cutting Tools" in self.data[i].get("product_category", []):
            category = "Hair Cutting Tools"
        elif "Hair Styling Tools" in self.data[i].get("product_category", []):
            category = "Hair Styling Tools"
        else:
            category = ([None, None] + self.data[i].get("product_category", []))[-2]

            # "Foot Care" -> "Callus Shavers"
            # "Electric Massagers" -> "Electric Handheld Massagers"
            # "Shaving, Waxing & Beard Care" -> "Scissors" -> "Moustache & Beard"
            # "Shaving, Waxing & Beard Care"
            # "Shaving, Waxing & Beard Care" -> "Shaving & Hair Removal"
        # gender = ""
        # if "Health & Personal Care" in self.data[i].get('product_category', []):
        #     gender = "Male"
        # elif "Beauty" in self.data[i].get('product_category', []):
        #     gender = "Female"
        print(
            [
                w if w not in category else ""
                for w in self.data[i]["product_brand"].lower().split()
            ]
        )
        prod_info = pd.DataFrame(
            [
                {
                    "ASIN": self.data[i]["product_pid"],
                    "Marketplace": self.data[i]["marketplace"],
                    "Title": self.data[i]["product_name"],
                    "Brand": " ".join(
                        [
                            w if w not in category else ""
                            for w in self.data[i]["product_brand"].split()
                        ]
                    ).title(),
                    "Original Price": self.data[i]["product_original_price"],
                    "Latest Selling Price": price,
                    "Last Day Selling Price": last_price,
                    "Latest BSR": bsr,
                    "Last Day BSR": last_bsr,
                    # "Estimated Sales": int(
                    #     0.8 * estimate_sales(float(price), float(bsr))
                    # )
                    # if price and bsr
                    # else "",
                    "Assured": assured,
                    "Availability": len(avail_days)
                    / len(avail_days.union(unavail_days))
                    if len(avail_days) > 0
                    else 0,
                    "Reviews": total_ratings,
                    "Ratings": ratings,
                    "Sentiment Score": "NA",
                    "Details Changed": "NA",
                    "Category": category,
                    # "Gender": gender
                }
            ]
        )

        # print(prod_move)
        prod_move = prod_move.set_index("Date")
        # print(prod_move)
        # print(prod_move.index)
        idx = pd.period_range("11-07-2022", "11-09-2022")
        idx = idx.strftime("%Y-%m-%d")
        # print(idx)
        prod_move = prod_move.reindex(idx, fill_value=None)
        prod_move = prod_move.reset_index()
        prod_move = prod_move.rename(columns={"index": "Date"})
        prod_move["ASIN"] = self.data[i]["product_pid"]

        return prod_info, prod_move

    def convert(self):
        # needed = {
        #     "B082PR8VWD",
        #     "B08GFR3Z61",
        #     "B07X5THKBZ",
        #     "B089FKM43P",
        #     'B07X2QPDH7',
        #     'B07TKKDD6W',
        #     'B082PV6HXD',
        #     'B07VFHDG9B',
        #     'B08GF7XNKJ',
        #     'B082PS71FM',
        #     'B083NSGRYB',
        #     'B081L2VD68',
        #     'B07VB8BHN6',
        #     'B07ZH7XHV8',
        #     'B081L17T11',
        #     'B08SRKK46Y',
        #     'B081L57FKW',
        #     'B07VB963G2',
        #     'B07VDFLHL2',
        #     'B08GFMGTR5',
        #     'B082NYYRJQ',
        #     'B082PRTPMP',
        #     'B082PV6HXD',
        #     'B081F8TF1N',
        # }
        # print(len(needed))
        for i in tqdm(range(len(self.data))):
            # if self.data[i]['product_pid'] in needed:
            prod_info, prod_move = self.convert_doc(i)
            # print(prod_info)
            # print(prod_move)
            # exit()
            if prod_info is not None and prod_move is not None:
                self.prod_infos = self.prod_infos.append(prod_info, ignore_index=True)
                self.prod_moves = self.prod_moves.append(prod_move, ignore_index=True)
                # if i > 5:
                #     break

    def save(self, info_file, move_file):
        self.prod_infos.to_csv(info_file, index=False)
        self.prod_moves.to_csv(move_file, index=False)


if __name__ == "__main__":
    # cvt = Converter('flipkart_marketplace_scraping_havells_11_02_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("flipkart_prod_info.csv", "flipkart_prod_move.csv")

    cvt = Converter(
        "../../data/InputData/nycil_powder/flipkart_marketplace_scraping/product_data.json",
        # "../../sentiment_analysis/XGBoost_Model_Nycil_Powder.json",
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prod_info.csv",
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prod_move.csv",
    )

    # cvt = Converter(
    #     "../../data/InputData/havells_light/flipkart_marketplace_scraping/product_data.json",
    #     "../../sentiment_analysis/XGBoost_Model_havells_light.json",
    # )
    # cvt.convert()
    # cvt.save(
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping/flipkart_prod_info.csv",
    #     "../../data/OutputData/havells_light/flipkart_marketplace_scraping/flipkart_prod_move.csv",
    # )

    # cvt = Converter('../../data/InputData/lloyd_washing_machine/flipkart_marketplace_scraping/product_data.json'
    # )# , "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping/flipkart_prod_info.csv", "../../data/OutputData/lloyd_washing_machine/flipkart_marketplace_scraping/flipkart_prod_move.csv")

    # cvt = Converter('../../data/InputData/lloyd_ac/flipkart_marketplace_scraping/product_data.json', "../../sentiment_analysis/XGBoost_Model_AC.json")
    # cvt.convert()
    # cvt.save("../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping/flipkart_prod_info.csv", "../../data/OutputData/lloyd_ac/flipkart_marketplace_scraping/flipkart_prod_move.csv")

    # cvt = Converter('../../data/InputData/lloyd_refrigerator/flipkart_marketplace_scraping/product_data.json'
    # )# , "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping/flipkart_prod_info.csv", "../../data/OutputData/lloyd_refrigerator/flipkart_marketplace_scraping/flipkart_prod_move.csv")

    # cvt = Converter('../pureit/flipkart_marketplace_scraping_pureit_16_05_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../pureit/flipkart_prod_info.csv", "../pureit/flipkart_prod_move.csv")

    # cvt = Converter('../shell/flipkart_marketplace_scraping_shell_01_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../shell/flipkart_prod_info.csv", "../shell/flipkart_prod_move.csv")

    # cvt = Converter('../duracell/flipkart_marketplace_scraping_duracell_02_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../duracell/flipkart_prod_info.csv", "../duracell/flipkart_prod_move.csv")

    # cvt = Converter('../havells_fans/flipkart_marketplace_scraping_havells_fans_31_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../havells_fans/flipkart_prod_info.csv", "../havells_fans/flipkart_prod_move.csv")

    # cvt = Converter('../havells_ac/flipkart_marketplace_scraping_havells_ac_31_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../havells_ac/flipkart_prod_info.csv", "../havells_ac/flipkart_prod_move.csv")

    # cvt = Converter('../ghadi/flipkart_marketplace_scraping_ghadi_05_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../ghadi/flipkart_prod_info.csv", "../ghadi/flipkart_prod_move.csv")

    # cvt = Converter('../kohler/flipkart_marketplace_scraping_kohler_11_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../kohler/flipkart_prod_info.csv", "../kohler/flipkart_prod_move.csv")

    # cvt = Converter('../park_avenue/flipkart_marketplace_scraping_park_avenue_21_03_22/product_data.json', "../park_avenue/XGBoost_Model_Park_Avenue.json")
    # cvt.convert()
    # cvt.save("../park_avenue/flipkart_prod_info.csv", "../park_avenue/flipkart_prod_move.csv")

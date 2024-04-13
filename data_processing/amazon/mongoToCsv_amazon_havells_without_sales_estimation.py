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

    #        self.estimation_model = XGBRegressor()
    #        self.estimation_model.load_model(model)
    # with open(model, 'rb') as f:
    #     self.estimation_model = pickle.load(f)

    def convert_doc(self, i):
        # if len(self.data[i]["product_sale_price"]) < 30:
        #     return None, None

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
            """
            # Havells
            e1 = re.search("#[0-9,]+ in Beauty", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Health & Personal Care", sbsr)
            """
            e1 = re.search("#[0-9,]+ in Men's Kurta Sets", sbsr) or re.search("#[0-9,]+ in Men's Kurtas", sbsr) or re.search("#[0-9,]+ in Men's Nehru Jackets & Vests", sbsr) or re.search("#[0-9,]+ in Men's Sherwani", sbsr)
            # e1 = re.search("#[0-9,]+ in Powder Detergent", sbsr)
            # e1 = re.search("#[0-9,]+ in Washing Machines & Dryers", sbsr)
            # e1 = re.search("#[0-9,]+ in Refrigerators", sbsr)
            # e1 = re.search("#[0-9,]+ in Stevia", sbsr) or re.search("#[0-9,]+ in Diabetes Care", sbsr)

            """
            # Refrigerator
            e1 = re.search("#[0-9,]+ in Refrigerators", sbsr)
            """

            """
            # Washing Machine
            e1 = re.search("#[0-9,]+ in Washing Machines & Dryers", sbsr)
            """

            """
            # Pureit
            e1 = re.search("#[0-9,]+ in Water Coolers, Filters & Cartridges", sbsr)
            """

            """
            # Shell
            e1 = re.search("#[0-9,]+ in Engine Oils for Cars", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Engine Oils for Motorbikes", sbsr)
            """
            """
            # Duracell
            e1 = re.search("#[0-9,]+ in Camera Batteries", sbsr)
            """
            """
            # Ferrero
            e1 = re.search("#[0-9,]+ in Chocolate Covered Nuts", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Chocolate Packets & Boxes", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Chocolate Gifts", sbsr)
            """
            """
            # Kohler
            e1 = re.search("#[0-9,]+ in Overhead Showers", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Handheld Showers", sbsr)
            if e1 is None:
                e1 = re.search("#[0-9,]+ in Shower Arms", sbsr)
            """
            """
            # Havells Fans
            e1 = re.search("#[0-9,]+ in Home & Kitchen", sbsr)
            """
            """
            # Havells ACs
            e1 = re.search("#[0-9,]+ in Air Conditioners", sbsr)
            """
            """
            # Ghadi
            e1 = re.search("#[0-9,]+ in Health & Personal Care", sbsr)
            """
            """
            # Nestle
            e1 = re.search("#[0-9,]+ in Grocery & Gourmet Foods", sbsr)
            """
            """
            # Park Avenue
            e1 = re.search("#[0-9,]+ in Beauty", sbsr)
            """
            """
            # Bajaj Mixer
            e1 = re.search("#[0-9,]+ in Home & Kitchen", sbsr)
            """
            """
            # Blue Heaven
            e1 = re.search("#[0-9,]+ in Beauty", sbsr)
            """
            """
            # Nature Essence
            e1 = re.search("#[0-9,]+ in Beauty", sbsr)
            """
            """
            # Eno
            e1 = re.search("#[0-9,]+ in Digestion & Nausea", sbsr)
            """
            """
            # Nescafe Coffee Maker
            e1 = re.search("#[0-9,]+ in Home & Kitchen", sbsr)
            """
            """
            # Nescafe Gold Coffee
            e1 = re.search("#[0-9,]+ in Grocery & Gourmet Foods", sbsr)
            """
            """
            # Ceregrow / Nestle Cornflakes / Kitkat / Maggi / Nescafe Sunrise
            e1 = re.search("#[0-9,]+ in Grocery & Gourmet Foods", sbsr)
            """
            e2 = re.search("#[0-9,]+", e1.group())
            if e2 is None:
                return None
            return int(e2.group().replace("#", "").replace(",", ""))

        # estimation_model = pickle.load(open('XGBoost_Model.sav', 'rb'))

        # def estimate_sales(price, bsr):
        #     # print(bsr, type(bsr), np.isnan(bsr))
        #     try:
        #         if bsr is None or np.isnan(bsr):
        #             return None
        #         elif bsr == 1:
        #             return 110
        #         elif bsr == 2:
        #             return 106
        #         elif bsr == 3:
        #             return 102
        #         elif bsr == 4:
        #             return 99
        #         elif bsr == 5:
        #             return 96
        #         elif bsr == 6:
        #             return 93
        #         elif bsr == 7:
        #             return 90
        #         elif bsr == 8:
        #             return 88
        #         elif bsr == 9:
        #             return 86
        #         elif bsr == 10:
        #             return 84
        #         elif bsr == 11:
        #             return 82
        #     except:
        #         print(bsr)
        #     # print(price, bsr)
        #     sales = self.estimation_model.predict(np.array([[price, bsr]]))
        #     # if bsr > 6000:
        #     #     sales = 0
        #     # sales = self.estimation_model([bsr])
        #     # print(sales)
        #     return max(round(sales[0]), 0)

        # def estimate_sales_with_df(price_bsr_df):
        #     return self.estimation_model.predict(price_bsr_df).round()

        self.data[i]["product_sale_price"] = sorted(
            self.data[i]["product_sale_price"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_best_seller_rank"] = sorted(
            self.data[i]["product_best_seller_rank"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_availability"] = sorted(
            self.data[i]["product_availability"],
            key=lambda d: datetime.strptime(d["time"], "%Y-%m-%d %H:%M:%S"),
        )
        self.data[i]["product_fullfilled"] = sorted(
            self.data[i]["product_fullfilled"],
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
                        "ASIN": self.data[i]["product_asin"],
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
        prod_price_move = prod_price_move.append(price_list, ignore_index=True)
        # print(prod_price_move)
        prod_price_move = pd.DataFrame(
            prod_price_move.groupby(["ASIN", "Date"])["Selling Price"].agg(
                lambda x: x.mean(skipna=False)
            )
        )
        # print(prod_price_move)
        prod_price_move = prod_price_move.reset_index()
        ###
        # prod_price_move = prod_price_move.set_index('Date')
        # idx = pd.period_range('03-12-2022', '05-05-2022')
        # idx = idx.strftime("%d-%m-%Y")
        # prod_price_move = prod_price_move.reindex(idx, fill_value=None)
        # prod_price_move = prod_price_move.reset_index()
        # prod_price_move = prod_price_move.rename(columns={'index': 'Date'})
        # prod_price_move['ASIN'] = self.data[i]['product_asin']
        ###
        prod_price_move["Date"] = pd.to_datetime(
            prod_price_move["Date"], format="%d-%m-%Y"
        )
        prod_price_move = prod_price_move.sort_values("Date")
        prod_price_move["Date"] = prod_price_move["Date"].dt.strftime("%Y-%m-%d")
        prod_price_move = prod_price_move.reset_index()
        prod_price_move = prod_price_move.drop(columns="index")

        # print(prod_price_move)
        # print(type(prod_price_move))
        # exit()
        bsr = None
        last_bsr = None
        bsr_list = []
        last_day = None
        day = None
        try:
            for b in self.data[i]["product_best_seller_rank"]:
                try:
                    b["value"] = float(convert_string_to_bsr(b["value"]))
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
                        "ASIN": self.data[i]["product_asin"],
                        "BSR": b["value"],
                        # 'Last Known BSR': bsr
                    }
                )
                if b["value"] is not None and not np.isnan(b["value"]):
                    # if self.data[i]['product_asin'] == "B085DJ8ZY3":
                    #     print(b['value'])
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
        prod_bsr_move = prod_bsr_move.append(bsr_list, ignore_index=True)
        # print(prod_bsr_move)
        prod_bsr_move = pd.DataFrame(
            prod_bsr_move.groupby(["ASIN", "Date"])["BSR"].agg(
                lambda x: x.mean(skipna=False)
            )
        )
        prod_bsr_move = prod_bsr_move.reset_index()
        ###
        # prod_bsr_move = prod_bsr_move.set_index('Date')
        # idx = pd.period_range('03-12-2022', '05-05-2022')
        # idx = idx.strftime("%d-%m-%Y")
        # prod_bsr_move = prod_bsr_move.reindex(idx, fill_value=None)
        # prod_bsr_move = prod_bsr_move.reset_index()
        # prod_bsr_move = prod_bsr_move.rename(columns={'index': 'Date'})
        # prod_bsr_move['ASIN'] = self.data[i]['product_asin']
        ###
        prod_bsr_move["Date"] = pd.to_datetime(prod_bsr_move["Date"], format="%d-%m-%Y")
        prod_bsr_move = prod_bsr_move.sort_values("Date")
        prod_bsr_move["Date"] = prod_bsr_move["Date"].dt.strftime("%Y-%m-%d")
        prod_bsr_move = prod_bsr_move.reset_index()
        prod_bsr_move = prod_bsr_move.drop(columns="index")

        fulfilled = None
        try:
            for f in self.data[i]["product_fullfilled"]:
                if f["value"] is not None:
                    fulfilled = f["value"]
        except:
            pass

        try:
            # Avail Loop
            avail_list = []
            for a in self.data[i]["product_availability"]:
                try:
                    a["value"] = (
                        False
                        if a["value"]
                        in ["P", "Currently unavailable", "Currently Unavailable"]
                        else True
                    )
                except:
                    a["value"] = False
                a["time"] = convert_string_to_day(a["time"])
                avail_list.append(
                    {
                        "ASIN": self.data[i]["product_asin"],
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
            ###
            # prod_avail_move = prod_avail_move.set_index('Date')
            # idx = pd.period_range('03-12-2022', '05-05-2022')
            # idx = idx.strftime("%d-%m-%Y")
            # prod_avail_move = prod_avail_move.reindex(idx, fill_value=None)
            # prod_avail_move = prod_avail_move.reset_index()
            # prod_avail_move = prod_avail_move.rename(columns={'index': 'Date'})
            # prod_avail_move['ASIN'] = self.data[i]['product_asin']
            ###
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

        # prod_move = pd.DataFrame(columns=['ASIN', 'Date', 'Selling Price', 'Last Known Selling Price', 'BSR', 'Last Known BSR', 'Availability'])
        prod_move = prod_price_move.merge(
            prod_bsr_move, how="inner", on=["ASIN", "Date"]
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
        prod_move["Estimated Sales"] = 0
        # prod_move['Estimated Sales'] = prod_move[['Selling Price', 'BSR']].apply(lambda x: estimate_sales(x[0], x[1]))
        # prod_move['Estimated Sales'] = list(map(estimate_sales, prod_move['Selling Price'], prod_move['BSR']))
        # prod_move['Estimated Sales'] = prod_move.apply(lambda x: estimate_sales(x['Selling Price'], x['BSR']), axis=1)
        # try:
        # prod_move['Estimated Sales'] = estimate_sales_with_df(prod_move[['Selling Price', 'BSR']])
        #        for j in range(len(prod_move)):
        #            prod_move.loc[j, 'Estimated Sales'] = estimate_sales(prod_move.loc[j, 'Selling Price'], prod_move.loc[j, 'BSR'])
        # prod_move['Estimated Sales'] = prod_move.apply(lambda x: "NA" if x['Selling Price'] is None or x['BSR'] is None else x['Estimated Sales'])
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
            if "Blue Star" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Blue Star"
            elif "Eureka Forbes" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Eureka Forbes"
            elif "Hul Pureit" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Hul Pureit"
            elif "Kent" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Kent"
            elif "Livpure" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Livpure"
            elif "Lloyd" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Lloyd"
            elif (
                "Voltas" in self.data[i]["product_name"]
                and "Voltas Beko" not in self.data[i]["product_name"]
            ):
                self.data[i]["product_brand"] = "Voltas"
            elif "Panasonic" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Panasonic"
            elif "Haier" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Haier"
            elif "LG" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "LG"
            elif "Daikin" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Daikin"
            elif "Hitachi" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Hitachi"
            elif "Samsung" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Samsung"
            elif "Whirlpool" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Whirlpool"
            elif "Liebherr" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Liebherr"
            elif "Voltas Beko" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Voltas Beko"
            elif "Bosch" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Bosch"
            elif "Havells" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Havells"
            elif "Philips" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "PHILIPS"
            elif "Syska" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "SYSKA"
            elif "Crompton" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Crompton"
            elif "Orient Electric" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Orient Electric"
            elif "Bajaj" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Bajaj"
            elif "NYCIL" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "NYCIL"
            elif "Candid Prickly" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Candid Prickly"
            elif "Navratna" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Navratna"
            elif "Dermi cool" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Dermi cool"
            elif "Shower To Shower" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Shower To Shower"
            elif "Surf Excel" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Surf Excel"
            elif "Tide" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Tide"
            elif "Ariel" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Ariel"
            elif "RIN" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "RIN"
            elif "BIBA" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "BIBA"
            elif "W for Woman" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "W for Woman"
            elif "Fabindia" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Fabindia"
            elif "Manyavar" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Manyavar"
            elif "VASTRAMAY" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "VASTRAMAY"
            elif "Ethnix by Raymond" in self.data[i]["product_name"]:
                self.data[i]["product_brand"] = "Ethnix by Raymond"
            else:
                self.data[i]["product_brand"] = self.data[i]["product_name"].split()[0]

        # print(self.data[i]['product_total_reviews'][-1]['value'].split()[0].replace(',', ''))
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
        # print(i, self.data[i]['product_asin'])

        prod_move = prod_move.merge(prod_avail_move, how="inner", on=["ASIN", "Date"])
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
            category = ([None] + self.data[i].get("product_category", []))[-1]

            # "Foot Care" -> "Callus Shavers"
            # "Electric Massagers" -> "Electric Handheld Massagers"
            # "Shaving, Waxing & Beard Care" -> "Scissors" -> "Moustache & Beard"
            # "Shaving, Waxing & Beard Care"
            # "Shaving, Waxing & Beard Care" -> "Shaving & Hair Removal"
        gender = ""
        # if "Health & Personal Care" in self.data[i].get('product_category', []):
        #     gender = "Male"
        # elif "Beauty" in self.data[i].get('product_category', []):
        #     gender = "Female"
        prod_info = pd.DataFrame(
            [
                {
                    "ASIN": self.data[i]["product_asin"],
                    "Title": self.data[i]["product_name"],
                    "Brand": " ".join(
                        self.data[i]["product_brand"].lower().split()
                    ).title(),
                    "Original Price": self.data[i]["product_original_price"],
                    "Latest Selling Price": price,
                    "Last Day Selling Price": last_price,
                    "Latest BSR": bsr,
                    "Last Day BSR": last_bsr,
                    "Estimated Sales": 0,  # estimate_sales(float(price), float(bsr)) if price and bsr else "NA",
                    "Fulfilled": fulfilled,
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

        # print(prod_move)
        # prod_move = prod_move.set_index('Date')
        # # print(prod_move)
        # # print(prod_move.index)
        # idx = pd.period_range('03-02-2022', '03-31-2022')
        # idx = idx.strftime("%Y-%m-%d")
        # # print(idx)
        # prod_move = prod_move.reindex(idx, fill_value=None)
        # prod_move = prod_move.reset_index()
        # prod_move = prod_move.rename(columns={'index': 'Date'})
        # prod_move['ASIN'] = self.data[i]['product_asin']

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
            # if self.data[i]['product_asin'] in needed:
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
    # cvt = Converter('amazon_marketplace_scraping_havells_11_02_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("amazon_prod_info.csv", "amazon_prod_move.csv")

    cvt = Converter(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/product_data.json"
    )  # , "XGBoost_Model_Havells.json")
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_prod_info.csv",
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_prod_move.csv",
    )

    # cvt = Converter('../shell/amazon_marketplace_scraping_shell_01_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../shell/amazon_prod_info.csv", "../shell/amazon_prod_move.csv")

    # cvt = Converter('../duracell/amazon_marketplace_scraping_duracell_02_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../duracell/amazon_prod_info.csv", "../duracell/amazon_prod_move.csv")

    # cvt = Converter('../hafele/amazon_marketplace_scraping_hafele_21_02_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../hafele/amazon_prod_info.csv", "../hafele/amazon_prod_move.csv")

    # cvt = Converter('../ferrero/amazon_marketplace_scraping_ferrero_02_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../ferrero/amazon_prod_info.csv", "../ferrero/amazon_prod_move.csv")

    # cvt = Converter('../kohler/amazon_marketplace_scraping_kohler_03_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../kohler/amazon_prod_info.csv", "../kohler/amazon_prod_move.csv")

    # cvt = Converter('../havells_fans/amazon_marketplace_scraping_havells_fans_31_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../havells_fans/amazon_prod_info.csv", "../havells_fans/amazon_prod_move.csv")

    # cvt = Converter('../havells_ac/amazon_marketplace_scraping_havells_ac_31_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../havells_ac/amazon_prod_info.csv", "../havells_ac/amazon_prod_move.csv")

    # cvt = Converter('../ghadi/amazon_marketplace_scraping_ghadi_05_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../ghadi/amazon_prod_info.csv", "../ghadi/amazon_prod_move.csv")

    # cvt = Converter('../nestle/amazon_marketplace_scraping_nestle_06_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../nestle/amazon_prod_info.csv", "../nestle/amazon_prod_move.csv")

    # cvt = Converter('../park_avenue/amazon_marketplace_scraping_park_avenue_21_03_22/product_data.json', "../park_avenue/Cubic_Spline_Model_Park_Avenue.pickle")
    # cvt.convert()
    # cvt.save("../park_avenue/amazon_prod_info.csv", "../park_avenue/amazon_prod_move.csv")

    # cvt = Converter('../bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_27_03_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../bajaj_mixer/amazon_prod_info.csv", "../bajaj_mixer/amazon_prod_move.csv")

    # cvt = Converter('../blue_heaven/amazon_marketplace_scraping_blue_heaven_06_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../blue_heaven/amazon_prod_info.csv", "../blue_heaven/amazon_prod_move.csv")

    # cvt = Converter('../nature_essence/amazon_marketplace_scraping_nature_essence_06_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../nature_essence/amazon_prod_info.csv", "../nature_essence/amazon_prod_move.csv")

    # cvt = Converter('../eno/amazon_marketplace_scraping_eno_12_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../eno/amazon_prod_info.csv", "../eno/amazon_prod_move.csv")

    # cvt = Converter('../nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../nescafe_coffee_maker/amazon_prod_info.csv", "../nescafe_coffee_maker/amazon_prod_move.csv")

    # cvt = Converter('../nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../nescafe_gold/amazon_prod_info.csv", "../nescafe_gold/amazon_prod_move.csv")

    # cvt = Converter('../ceregrow/amazon_marketplace_scraping_ceregrow_18_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../ceregrow/amazon_prod_info.csv", "../ceregrow/amazon_prod_move.csv")

    # cvt = Converter('../nestle_cornflakes/amazon_marketplace_scraping_nestle_cornflakes_16_05_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../nestle_cornflakes/amazon_prod_info.csv", "../nestle_cornflakes/amazon_prod_move.csv")

    # cvt = Converter('../kitkat/amazon_marketplace_scraping_kitkat_18_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../kitkat/amazon_prod_info.csv", "../kitkat/amazon_prod_move.csv")

    # cvt = Converter('../maggi/amazon_marketplace_scraping_maggi_18_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../maggi/amazon_prod_info.csv", "../maggi/amazon_prod_move.csv")

    # cvt = Converter('../nescafe_sunrise/amazon_marketplace_scraping_nescafe_sunrise_18_04_22/product_data.json', "XGBoost_Model_Havells.json")
    # cvt.convert()
    # cvt.save("../nescafe_sunrise/amazon_prod_info.csv", "../nescafe_sunrise/amazon_prod_move.csv")

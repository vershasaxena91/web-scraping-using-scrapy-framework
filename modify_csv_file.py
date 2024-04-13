import pandas as pd
import numpy as np
from math import isnan

# File path
file_path = "amazon_product_scraping/data/OutputData/amazon_data.csv"


def modify_csv_file(file_path):
    # Read csv file
    df = pd.read_csv(file_path)

    # Convert type of columns (string) into list
    df["product_sale_price"] = [eval(i) for i in df["product_sale_price"]]
    df["product_fullfilled"] = [eval(i) for i in df["product_fullfilled"]]
    df["product_availability"] = [eval(i) for i in df["product_availability"]]
    icons = list(df["product_icons"])
    icon = []
    for i in icons:
        if type(i) == str:
            icon.append(eval(i))
        else:
            icon.append(i)
    df["product_icons"] = icon
    df["product_best_seller_rank"] = [eval(i) for i in df["product_best_seller_rank"]]
    df["product_subscription_discount"] = [
        eval(i) for i in df["product_subscription_discount"]
    ]

    # Make a column "Price" with latest sale price
    sale_price = [i[-1]["value"] for i in df["product_sale_price"]]
    sale_price = [str(i).replace(",", "") for i in sale_price]
    price = []
    for i in sale_price:
        if i != "NA":
            price.append(float(i))
        else:
            price.append(np.nan)
    df["Price"] = price

    # Make a column "Range of Price"
    price_range = []
    for i in price:
        if i != "NA":
            if i < 100:
                price_range.append("<100")
            elif i >= 100 and i <= 150:
                price_range.append("100-150")
            elif i >= 151 and i <= 200:
                price_range.append("151-200")
            elif i >= 200 and i <= 300:
                price_range.append("200-300")
            elif i > 301:
                price_range.append(">301")
            else:
                price_range.append(i)
        else:
            price_range.append(i)
    df["Range of Price"] = price_range

    # Make a column "Best Seller Range" with latest BSR
    bsr = [i[-1]["value"] for i in df["product_best_seller_rank"]]
    Bsr = []
    for i in bsr:
        if i != "NA":
            x = i.split("#")
            y = [j for j in x if "Shampoos" in j]
            if len(y) == 1:
                z = y[0].split(" ")[0]
                w = z.replace(",", "")
                u = int(w)
                Bsr.append(u)
            else:
                Bsr.append(np.nan)
        else:
            Bsr.append(np.nan)
    df["Best Seller Rank"] = Bsr

    # Make a column "Range of BSR" for Top 100 BSR
    bsr_range_1 = []
    for i in Bsr:
        if i != "NA":
            if i <= 10 and i >= 1:
                bsr_range_1.append("1 to 10")
            elif i >= 11 and i <= 20:
                bsr_range_1.append("11 to 20")
            elif i >= 21 and i <= 50:
                bsr_range_1.append("21 to 50")
            elif i >= 51 and i <= 100:
                bsr_range_1.append("51 to 100")
            elif i > 100:
                bsr_range_1.append(">100")
            else:
                bsr_range_1.append(i)
        else:
            bsr_range_1.append(i)
    df["Range of BSR"] = bsr_range_1

    # Make a column "Range of BSR" for Top 1000 BSR
    bsr_range_2 = []
    for i in Bsr:
        if i != "NA":
            if i <= 100 and i >= 1:
                bsr_range_2.append("1-100")
            elif i >= 101 and i <= 200:
                bsr_range_2.append("101-200")
            elif i >= 201 and i <= 500:
                bsr_range_2.append("201-500")
            elif i >= 501 and i <= 1000:
                bsr_range_2.append("501-1000")
            elif i > 1000:
                bsr_range_2.append(">1000")
            else:
                bsr_range_2.append(i)
        else:
            bsr_range_2.append(i)
    df["BSR Range"] = bsr_range_2

    # Make a column "BSR" with string type
    df["BSR"] = list(df["Best Seller Rank"])
    df["BSR"] = [str(i) for i in df["BSR"]]

    # Make a column "Rating" with float type
    ratings = list(df["product_rating"])
    rating = []
    for i in ratings:
        if type(i) == str:
            x = i.split("out")
            rating.append(float(x[0]))
        else:
            rating.append(i)
    df["Rating"] = rating

    # Make a column "Reviews" with float type
    reviews = list(df["product_total_reviews"])
    review = []
    for i in reviews:
        if type(i) == str:
            x = i.split(" ")
            review.append(x[0])
        else:
            review.append(i)
    df["Reviews"] = review
    df["Reviews"] = [float(str(i).replace(",", "")) for i in df["Reviews"]]

    # Make a column "Fulfilled" with latest fulfilled value for FBA or Non FBA
    fulfilled = [i[-1]["value"] for i in df["product_fullfilled"]]
    fulfillment = []
    for i in fulfilled:
        if i == "Fulfilled":
            fulfillment.append("FBA")
        else:
            fulfillment.append("Non FBA")
    df["Fulfilled"] = fulfillment

    # Make a column "Refundable" for Refundable or Non Refundable
    refundable = []
    for i in df["product_icons"]:
        if "Not Returnable" or "Non-Returnable" in i:
            refundable.append("Non Refundable")
        elif "Returns Policy" or "10 Days Returnable" in i:
            refundable.append("Refundable")
    df["Refundable"] = refundable

    # Make a column discount with latest discount value
    discounts = [i[-1]["value"] for i in df["product_subscription_discount"]]
    discount = []
    for i in discounts:
        if i != "NA":
            discount.append(float(str(i).replace("%", "")))
        else:
            discount.append(np.nan)
    df["discount"] = discount

    # Remove comma from original price
    df["product_original_price"] = [
        str(i).replace(",", "") for i in df["product_original_price"]
    ]

    # Make a column "Original Price" with float type
    original_price = list(df["product_original_price"])
    mrp = []
    for i in original_price:
        if type(i) == str:
            mrp.append(float(i))
        else:
            mrp.append(i)
    df["Original Price"] = mrp

    # Make a column "Price Trend" with a list of all sale price values
    price_trend = []
    for i in df["product_sale_price"]:
        l = []
        for j in i:
            if j["value"] != "NA":
                l.append(float(j["value"].replace(",", "")))
            else:
                l.append(np.nan)
        price_trend.append(l)
    df["Price Trend"] = price_trend

    # Make a column "Selling Price" with latest non null value
    Price = []
    for i in price_trend:
        reverse_price_trend = i[::-1]
        s = np.all(np.isnan(i))
        if s == False:
            Price.append(next(j for j in reverse_price_trend if not isnan(j)))
        else:
            Price.append(np.nan)
    df["Selling Price"] = Price

    # Make a column "Listed Price" with modify original price
    mrp_price = []
    for i, j, k in zip(mrp, discount, Price):
        if isnan(i):
            if isnan(j) and not isnan(k):
                mrp_price.append(k)
            elif isnan(j) and isnan(k):
                mrp_price.append(i)
            elif not isnan(j) and not isnan(k):
                m = round((k * 100) / (100 - j), 2)
                mrp_price.append(m)
            else:
                mrp_price.append(i)
        else:
            mrp_price.append(i)
    df["Listed Price"] = mrp_price

    # Make a column "Discount" with modify discount value
    dis = []
    for i, j, k in zip(mrp_price, discount, Price):
        if isnan(j):
            if i == k and not isnan(i) and not isnan(k):
                dis.append(0)
            elif i != k and not isnan(i) and not isnan(k):
                d = round(((i - k) * 100) / i, 2)
                dis.append(d)
            elif isnan(i) and isnan(k):
                dis.append(j)
            else:
                dis.append(j)
        else:
            if isnan(i) and isnan(k):
                dis.append(np.nan)
            else:
                dis.append(j)
    df["Discount"] = dis

    # Modify brand
    name = list(df["product_name"])
    brand = list(df["product_brand"])
    Brand = []
    for i in range(0, len(name)):
        if type(brand[i]) != str:
            if "L'Oreal Paris" or "Loreal" or "L'Oreal" or "L'oreal Paris" in name[i]:
                Brand.append("L'Oreal Paris")
            elif "Head & Shoulders" or "Head and Shoulders" in name[i]:
                Brand.append("Head & Shoulders")
            elif "BIOTIQUE" or "Biotique" in name[i]:
                Brand.append("Biotique")
            elif "Tresemme" or "TRESemme" in name[i]:
                Brand.append("TRESemme")
            elif "Dove" or "DOVE" in name[i]:
                Brand.append("Dove")
            elif "MATRIX" in name[i]:
                Brand.append("Matrix")
            elif "BIOLAGE" in name[i]:
                Brand.append("Biolage")
            elif "Pantene" in name[i]:
                Brand.append("Pantene")
            elif "Indulekha" in name[i]:
                Brand.append("Indulekha")
            elif "Nyle" in name[i]:
                Brand.append("Nyle")
            elif "Sunsilk" in name[i]:
                Brand.append("Sunsilk")
            else:
                Brand.append("Others")
        else:
            Brand.append(brand[i])
    df["product_brand"] = Brand

    # Make a column "Brand" included 5 top brands and others
    others_brand = []
    for i in Brand:
        if (
            i != "L'Oreal Paris"
            and i != "Head & Shoulders"
            and i != "Biotique"
            and i != "TRESemme"
            and i != "Dove"
        ):
            others_brand.append("Others")
        else:
            others_brand.append(i)
    df["Brand"] = others_brand

    # Rename a column "product_name" into "Product Title"
    df = df.rename(columns={"product_name": "Product Title"})

    # Rename a column "product_brand" into "Product Brand"
    df = df.rename(columns={"product_brand": "Product Brand"})

    # Save this in csv file
    df.to_csv("amazon_product_scraping/data/OutputData/updated_data.csv", index=False)


# Call the function
modify_csv_file(file_path)

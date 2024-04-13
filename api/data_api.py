from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)


@app.route("/getAmazon", methods=["GET"])
def getAmazon():
    asin = request.args.get("asin")
    db = MongoClient("mongodb://localhost:27017")["amazon_marketplace_scraping_pureit"]
    prod = db["product_data"].find_one({"product_asin": asin})

    if prod is None:
        return jsonify(status=400, error="Product not being tracked.")

    rating = None
    num_ratings = None
    sale_price = None
    original_price = None

    if "product_rating" in prod.keys():
        for d in prod["product_rating"]:
            if (
                isinstance(d, dict)
                and isinstance(d["value"], str)
                and d["value"] != "NA"
            ):
                rating = float(d["value"].split(" out ")[0])

    if "product_total_reviews" in prod.keys():
        for d in prod["product_total_reviews"]:
            if (
                isinstance(d, dict)
                and isinstance(d["value"], str)
                and d["value"] != "NA"
            ):
                num_ratings = int(d["value"].split()[0].replace(",", ""))

    if (rating is None) or (num_ratings is None):
        return jsonify(status=400, error="Product does not have a rating.")

    if "product_sale_price" in prod.keys():
        for d in prod["product_sale_price"]:
            if (
                isinstance(d, dict)
                and isinstance(d["value"], str)
                and d["value"] != "NA"
            ):
                sale_price = float(d["value"])

    if "product_original_price" in prod.keys():
        for d in prod["product_original_price"]:
            if (
                isinstance(d, dict)
                and isinstance(d["value"], str)
                and d["value"] != "NA"
            ):
                original_price = float(d["value"])

    # if (
    #     "product_original_price" in prod.keys()
    #     and isinstance(prod["product_original_price"], str)
    #     and prod["product_original_price"] != "NA"
    # ):
    #     original_price = float(prod["product_original_price"])
    # else:
    #     original_price = sale_price

    return jsonify(
        status=200,
        rating=rating,
        num_ratings=num_ratings,
        original_price=original_price,
        sale_price=sale_price,
    )


@app.route("/getFlipkart", methods=["GET"])
def getFlipkart():
    pid = request.args.get("pid")
    marketplace = request.args.get("marketplace")
    db = MongoClient("mongodb://localhost:27017")[
        "flipkart_marketplace_scraping_pureit"
    ]
    prod = db["product_data"].find_one({"product_pid": pid, "marketplace": marketplace})

    if prod is None:
        return jsonify(status=400, error="Product not being tracked.")

    rating = None
    num_ratings = None
    sale_price = None
    original_price = None

    if "product_rating" in prod.keys():
        for d in prod["product_rating"]:
            if isinstance(d["value"], str) and d["value"] != "NA":
                rating = float(d["value"].split(" out ")[0])

    if "product_total_ratings" in prod.keys():
        for d in prod["product_total_ratings"]:
            if isinstance(d["value"], str) and d["value"] != "NA":
                num_ratings = int(d["value"].split()[0].replace(",", ""))

    if (rating is None) or (num_ratings is None):
        return jsonify(status=400, error="Product does not have a rating.")

    if "product_sale_price" in prod.keys():
        for d in prod["product_sale_price"]:
            if (
                isinstance(d, dict)
                and isinstance(d["value"], str)
                and d["value"] != "NA"
            ):
                sale_price = float(d["value"])

    if (
        "product_original_price" in prod.keys()
        and isinstance(prod["product_original_price"], str)
        and prod["product_original_price"] != "NA"
    ):
        original_price = float(prod["product_original_price"])
    else:
        original_price = sale_price

    return jsonify(
        status=200,
        rating=rating,
        num_ratings=num_ratings,
        original_price=original_price,
        sale_price=sale_price,
    )


if __name__ == "__main__":
    app.run()

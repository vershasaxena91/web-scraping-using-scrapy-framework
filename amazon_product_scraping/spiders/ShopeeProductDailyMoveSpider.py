from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
from amazon_product_scraping.items import ShopeeProductDailyMovementItem


class ShopeeProductDailyMoveSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "ShopeeProductDailyMoveSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.ShopeeProductDailyMovementToMongoPipeline": 300
        }
    }

    def start_requests(self):
        if self.cold_run:
            for params in self.urls:
                self.add_to_failed("info", params)
                yield WebScrapingApiRequest(
                    url=params["url"],
                    callback=partial(self.parse_info, params),
                )
        else:
            for func, params in self.failed_urls:
                if func == "info":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_info, params),
                    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.success_counts = kwargs["success_counts"]
        self.urls = []
        self.mongo_db = kwargs["mongo_db"]
        self.time = kwargs["time"]

    def add_to_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)

    def remove_from_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)

    def parse_info(self, params, response):
        print(response.url, response.status)
        print(params)

        failed = False
        if response.status != 200:
            failed = True

        data = response.json()["data"]
        item = ShopeeProductDailyMovementItem()

        try:
            item["product_itemid"] = params["url"].split("itemid=")[1].split("&")[0]
            item["product_shopid"] = params["url"].split("shopid=")[1].split("&")[0]
            if len(item["product_itemid"]) == 0 or len(item["product_shopid"]) == 0:
                failed = True
        except:
            failed = True

        try:
            item["product_sale_price"] = {
                "time": self.time,
                "value": data["price"] / 100000000,
            }
            item["product_rating"] = {
                "time": self.time,
                "value": data["item_rating"]["rating_star"]
                if "item_rating" in data.keys()
                else "",
            }
            item["product_total_ratings"] = {
                "time": self.time,
                "value": data["item_rating"]["rating_count"]
                if "item_rating" in data.keys()
                else "",
            }
            item["product_total_likes"] = {
                "time": self.time,
                "value": data["liked_count"] if "liked_count" in data.keys() else "",
            }
            item["product_stock"] = {
                "time": self.time,
                "value": data["stock"] if "stock" in data.keys() else "",
            }
            item["product_sold"] = {
                "time": self.time,
                "value": data["sold"] if "sold" in data.keys() else "",
            }
            item["product_historical_sold"] = {
                "time": self.time,
                "value": data["historical_sold"]
                if "historical_sold" in data.keys()
                else "",
            }
            item["product_original_price"] = {
                "time": self.time,
                "value": data["price_before_discount"] / 100000000,
            }
            item["product_discount"] = {
                "time": self.time,
                "value": data["discount"],
            }
            # TODO Add More
        except:
            failed = True

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/{}_{}.json".format(
                        item["product_itemid"], item["product_shopid"]
                    ),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None

        else:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
            self.remove_from_failed("info", params)
            yield item

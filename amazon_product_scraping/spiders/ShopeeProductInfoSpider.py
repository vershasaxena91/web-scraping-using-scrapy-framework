from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
from amazon_product_scraping.items import ShopeeProductInfoItem
import datetime


class ShopeeProductInfoSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "ShopeeProductInfoSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.ShopeeProductInfoToMongoPipeline": 300
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
        def getAttributes(attributes):
            attributesPair = {}
            for x in attributes:
                attributesPair[x["name"]] = x["value"]
            return attributesPair

        def getCategories(categoriesList):
            categories = []
            for x in categoriesList:
                categories.append(x["display_name"])
            return categories

        def getVariations(variationsList):
            variations = []
            for x in variationsList:
                if len(x["name"]) > 0:
                    variations.append({"name": x["name"], "options": x["options"]})
            return variations

        print(response.url, response.status)
        print(params)

        failed = False
        if response.status != 200:
            failed = True

        data = response.json()["data"]
        item = ShopeeProductInfoItem()

        try:
            item["product_itemid"] = params["url"].split("itemid=")[1].split("&")[0]
            item["product_shopid"] = params["url"].split("shopid=")[1].split("&")[0]
            if len(item["product_itemid"]) == 0 or len(item["product_shopid"]) == 0:
                failed = True
        except:
            failed = True

        try:
            item["product_name"] = data["name"]

            item["product_brand"] = data["brand"] if "brand" in data.keys() else ""

            item["product_original_price"] = data["price_before_discount"] / 100000000

            item["product_description"] = (
                data["description"] if "description" in data.keys() else ""
            )

            item["product_categories"] = (
                getCategories(data["categories"]) if "categories" in data.keys() else []
            )

            item["product_variations"] = (
                getVariations(data["tier_variations"])
                if "tier_variations" in data.keys()
                else []
            )

            item["product_images"] = (
                len(data["images"])
                if "images" in data.keys() and isinstance(data["images"], list)
                else 0
            )

            item["product_videos"] = (
                len(data["video_info_list"])
                if "video_info_list" in data.keys()
                and isinstance(data["video_info_list"], list)
                else 0
            )

            item["product_attributes"] = (
                getAttributes(data["attributes"])
                if "attributes" in data.keys() and isinstance(data["attributes"], list)
                else {}
            )

            item["scraped_on"] = self.time
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

from amazon_product_scraping.items import (
    ShopeeShareOfSearchItem,
    ShopeeShareOfSearchRanksItem,
)
from math import ceil
from functools import partial
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from urllib.parse import unquote, quote


class ShopeeShareOfSearchSpider(WebScrapingApiSpider):

    # Make Debug = False while deploying
    debug = False
    handle_httpstatus_all = True
    name = "ShopeeShareOfSearchSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.ShopeeShareOfSearchPipeline": 300
        }
    }

    def start_requests(self):
        """
        This class method must return an iterable with the first Requests to crawl for this spider.
        """

        if self.cold_run:
            for url in self.urls:
                self.add_to_failed("count", {"url": url})

                yield WebScrapingApiRequest(
                    url=url,
                    callback=partial(self.parse_count, {"url": url}),
                )
        else:
            for func, params in self.failed_urls:
                if func == "count":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_count, params),
                    )
                elif func == "list":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_product_list, params),
                    )
                elif func == "data":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_product_data, params),
                    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.keywords = kwargs["keywords"]
        self.pages = kwargs["pages"]
        self.success_counts = kwargs["success_counts"]
        self.time = kwargs["time"]
        self.urls = []
        self.mongo_db = kwargs["mongo_db"]
        self.present_check_func = None

    def add_to_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)
            return True
        else:
            return False

    def remove_from_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)
            return True
        else:
            return False

    def parse_count(self, params, response):
        """
        A class method used to parse the response for each request, extract scraped data as dicts.

        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        count
            extracted count of products appearing in search
        """

        print(response.url, response.status)
        print(params)

        failed = False
        if response.status != 200:
            failed = True

        data = response.json()
        count = data["total_count"]
        per_page = 100
        print(count)

        print("**********\n Count:", count)

        if failed:
            if self.debug:
                with open(
                    "fails/SOS_{}.json".format("shopee_count_fail"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/SOS_{}.json".format("shopee_count_succ"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("count", params)
            yield None
            if isinstance(count, int):
                for i in range(0, min(self.pages, ceil(count / per_page))):
                    next_page_url = params["url"] + "&newest={}".format(i * 100)
                    self.add_to_failed("list", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(
                            self.parse_product_list, {"url": next_page_url}
                        ),
                        # meta={
                        #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                        # }
                    )

    def parse_product_list(self, params, response):
        """
        A class method used to parse the response for each request, extract scraped data as dicts.

        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dicts
            extract the scraped data as dicts
        """

        print(response.url, response.status)
        print(params)

        failed = False
        if response.status != 200:
            failed = True

        data = response.json()
        items = ShopeeShareOfSearchRanksItem()
        keyword = params["url"].split("&keyword=")[1].split("&")[0]
        page = int(params["url"].split("&newest=")[1].split("&")[0]) / 100
        rank = page * 100

        if not failed:
            prods = []
            for item in data["items"]:
                prods.append(
                    {"product_itemid": item["itemid"], "product_shopid": item["shopid"]}
                )
            if len(prods) == 0:
                failed = True

        if failed:
            if self.debug:
                with open(
                    "fails/SOS_shopee_list_fail_{}.json".format(page),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("list", params)

            i = 0
            product_ranks = []
            if self.debug:
                with open(
                    "fails/SOS_shopee_list_succ_{}.json".format(page),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            for prod in prods:
                i += 1
                if prod["product_itemid"] != "":
                    rank += 1
                    static = "https://shopee.co.id/api/v4/item/get"

                    product_ranks.append(
                        {
                            "product_itemid": prod["product_itemid"],
                            "product_shopid": prod["product_shopid"],
                            "keyword": keyword,
                            "product_rank": rank,
                        }
                    )
                    # print(rank, isSponsored)
                    if not self.present_check_func(
                        {
                            "product_itemid": prod["product_itemid"],
                            "product_shopid": prod["product_shopid"],
                        }
                    ):

                        url = "{}?itemid={}&shopid={}".format(
                            static, prod["product_itemid"], prod["product_shopid"]
                        )
                        # if isSponsored:
                        #     print('Sponsored', i, rank)
                        wrapper = {"url": url}
                        if self.add_to_failed("data", wrapper):
                            yield WebScrapingApiRequest(
                                url=url,
                                callback=partial(self.parse_product_data, wrapper),
                                # meta={
                                #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                                # }
                            )
            items["product_ranks"] = product_ranks
            yield items

    def parse_product_data(self, params, response):
        """
        A class method used to parse the response for each request, extract scraped data as dicts.

        Parameters
        ----------
        response : object
            represents an HTTP response

        Returns
        -------
        dicts
            extract the scraped data as dicts
        """

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
        item = ShopeeShareOfSearchItem()

        try:
            item["product_url"] = params["url"]
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
        except:
            failed = True

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/SOS_shopee_data_fail_{}_{}.json".format(
                        item["product_itemid"], item["product_shopid"]
                    ),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("data", params)

            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/SOS_shopee_data_succ_{}_{}.json".format(
                        item["product_itemid"], item["product_shopid"]
                    ),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)

            yield item
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[2]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[3]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[4]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span

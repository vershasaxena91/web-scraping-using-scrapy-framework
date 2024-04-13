from amazon_product_scraping.items import NykaaShareOfSearchItem
from math import ceil
from functools import partial
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from urllib.parse import unquote, quote
import datetime


class NykaaShareOfSearchSpider(WebScrapingApiSpider):

    # Make Debug = False while deploying
    debug = False
    handle_httpstatus_all = True
    name = "NykaaShareOfSearchSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.NykaaShareOfSearchPipeline": 300
        }
    }

    def start_requests(self):
        """
        This class method must return an iterable with the first Requests to crawl for this spider.
        """

        if self.cold_run:
            for url in self.urls:
                self.add_to_failed("list_data", {"url": url})

                yield WebScrapingApiRequest(
                    url=url,
                    callback=partial(self.parse_product_list_data, {"url": url}),
                )
        else:
            for func, params in self.failed_urls:
                if func == "list_data":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_product_list_data, params),
                    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.keywords = kwargs["keywords"]
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

    def parse_product_list_data(self, params, response):
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
        item = NykaaShareOfSearchItem()
        keyword = params["url"].split("&search=")[1].split("&")[0].replace("%20", " ")

        if not failed:
            i = 0
            products = []
            for product in data["response"]["products"]:
                i += 1
                if "id" in product.keys():
                    brand = (
                        product["brand_name"][0]
                        if product["brand_name"] != None
                        else "NA"
                    )
                    products.append(
                        {
                            "product_id": product["id"],
                            "product_url": product["product_url"],
                            "keyword": keyword,
                            "product_rank": i,
                            "product_name": product["name"],
                            "product_brand": brand,
                            "product_original_price": str(product["price"]),
                            # "product_categories": [i["name"] for i in product["primary_categories"].values()],
                            "scraped_on": self.time,
                            "product_sale_price": str(product["final_price"]),
                            "product_rating": str(product["rating"]),
                            "product_total_ratings": str(product["rating_count"]),
                            "product_stock": str(product["quantity"]),
                            "product_discount": str(product["discount"]),
                            "product_availability": str(product["in_stock"]),
                        }
                    )
            print(len(products))
            if len(products) == 0:
                failed = True

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/SOS_nykaa_data_fail_{}.json".format(item["product_id"]),
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
                    "fails/SOS_nykaa_data_succ_{}_{}.json".format(item["product_id"]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)

            item["product_details"] = products
            yield item

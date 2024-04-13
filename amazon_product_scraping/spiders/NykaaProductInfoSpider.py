from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from amazon_product_scraping.utils.NykaaScrapingHelper import NykaaScrapingHelper
import logging
from functools import partial
from amazon_product_scraping.items import NykaaProductInfoItem
import datetime

logger = logging.getLogger("scraper")
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)


class NykaaProductInfoSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "NykaaProductInfoSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.NykaaProductInfoToMongoPipeline": 300
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

        with open("{:%Y-%m-%d}.log".format(datetime.datetime.now()), "a") as f:
            f.write(
                "{}, {}\n".format(
                    response.url,
                    response.status,
                )
            )

        failed = False
        if response.status != 200:
            failed = True

        helper = NykaaScrapingHelper()
        item = NykaaProductInfoItem()

        if not failed:
            if self.debug:
                print("title")
            try:
                title = helper.get_title(response)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                title = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("brand")
            try:
                brand = helper.get_brand(response)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                brand = "NA"
                failed = True

        if self.debug:
            print("product_id")
        try:
            product_id = params["url"].split("/p/")[1]
            if product_id is None or product_id == "":
                failed = True
        except Exception:
            logging.error("Exception occurred", exc_info=True)
            product_id = "NA"
            failed = True

        # if not failed:
        #     if self.debug:
        #         print("description")
        #     try:
        #         product_description = helper.get_product_description(response)
        #     except Exception:
        #         logging.error("Exception occurred", exc_info=True)
        #         product_description = "NA"
        #         failed = True

        # if not failed:
        #     if self.debug:
        #         print("images")
        #     try:
        #         images = helper.get_images(response)
        #     except Exception:
        #         logging.error("Exception occurred", exc_info=True)
        #         images = []
        #         failed = True

        if not failed:
            item["product_name"] = title
            item["product_brand"] = brand
            # item["product_category"] = category
            item["product_id"] = product_id
            # item["product_description"] = product_description
            # item["product_image_urls"] = images

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/{}.json".format(item["product_id"]),
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

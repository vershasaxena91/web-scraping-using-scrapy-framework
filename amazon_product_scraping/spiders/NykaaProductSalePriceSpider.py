from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
from amazon_product_scraping.utils.NykaaScrapingHelper import NykaaScrapingHelper
from amazon_product_scraping.items import NykaaProductDailyMovementItem
import datetime


class NykaaProductSalePriceSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "NykaaProductSalePriceSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.NykaaProductDailyMovementToMongoPipeline": 300
        }
    }

    def start_requests(self):
        if self.cold_run:
            for params in self.urls:
                self.add_to_failed("movement", params)
                yield WebScrapingApiRequest(
                    url=params["url"],
                    callback=partial(self.parse_info, params),
                )
        else:
            for func, params in self.failed_urls:
                if func == "movement":
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
        item = NykaaProductDailyMovementItem()

        try:
            product_id = params["url"].split("/p/")[1]
            if product_id is None or product_id == "":
                failed = True
        except:
            product_id = "NA"
            failed = True

        if not failed:
            if self.debug:
                print("sale_price")
            try:
                sale_price = helper.get_sale_price(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                sale_price = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("original_price")
            try:
                original_price = helper.get_original_price(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                original_price = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("discount")
            try:
                discount = helper.get_discount(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                discount = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("rating")
            try:
                rating = helper.get_rating(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                rating = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("total_ratings")
            try:
                total_ratings = helper.get_total_ratings(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                total_ratings = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("total_reviews")
            try:
                total_reviews = helper.get_total_reviews(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                total_reviews = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("availability")
            try:
                availability = helper.get_availability(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                availability = "NA"
                failed = True

        if not failed:
            item["product_id"] = product_id
            item["product_sale_price"] = sale_price
            item["product_original_price"] = original_price
            item["product_availability"] = availability
            item["product_rating"] = rating
            item["product_total_ratings"] = total_ratings
            item["product_total_reviews"] = total_reviews
            item["product_discount"] = discount

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/{}.html".format(product_id), "w", encoding="utf-8"
                ) as f:
                    f.write(response.text)
            yield None

        else:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
            self.remove_from_failed("movement", params)
            yield item

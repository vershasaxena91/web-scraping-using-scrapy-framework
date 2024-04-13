from amazon_product_scraping.items import AmazonProductDailyMovementItem
from amazon_product_scraping.utils.AmazonScrapingHelper import AmazonScrapingHelper
import logging
from scrapy import signals
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
import datetime
import scrapy

logger = logging.getLogger("scraper")
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)


class AmazonProductSalePriceBSRSpider(scrapy.Spider):#WebScrapingApiSpider):
    """
    Amazon Product Sale Price BSR Spider.
    --
    Spider for scraping required Amazon Product Daily Movement (Price, BSR, Availability, Fulfilled Status, Rating, Reviews Count, Questions Count). e.g. (https://www.amazon.in/dp/B07D1HRHLV).

    Inputs
    ----------
    - cold_run : Boolean
        - Set to True if 1st run.

    - failed_urls : list
        - List of URLs & functions failed in previous run.
        - Required when cold_run = False

    - success_counts : dict
        - Dictionary to store all counts.
            - new : will be updated based on count of products found from product_list.
            - added : will be updated based on count of Daily Movement added for products found.

    - mongo_db : str
        - MongoDB Name

    - time : str
        - Timestamp

    Attributes
    ----------
    - debug : Boolean
        - Boolean value to indicate whether to print logs, store copy of html files, etc.
        - Set to True when testing.

    - handle_httpstatus_all : Boolean
        - True

    - name : str
        - Spider Name to identify the spider

    - rotate_user_agent : Boolean
        - True

    - custom_settings : dict
        - Custom Setting to select Pipeline, etc.
    """

    debug = False
    handle_httpstatus_all = True
    name = "AmazonProductSalePriceBSRSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.AmazonProductDailyMovementToMongoPipeline": 300
        }
    }

    def start_requests(self):
        """
        Function to send requests to start crawling.

        ---

        - When cold_run = True, uses urls generated in AmazonProductDailyMovementToMongoPipeline.open_spider.
        - When cold_run = False, uses urls from failed_urls.
        """

        if self.cold_run:
            # If running for the first time.
            for url in self.urls:
                self.add_to_failed("movement", {"url": url})
                yield scrapy.Request(#WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_movement, {"url": url})
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "movement":
                    yield scrapy.Request(#WebScrapingApiRequest(
                        url=params["url"], callback=partial(self.parse_movement, params)
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
        """
        Function to manage (add to) failed_urls list.

        Inputs
        ------
        - parser_func : str
            - String denoting parser function where url failed.

        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
        """

        # wrapper contains function name and parameters that failed the last time.
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)

    def remove_from_failed(self, parser_func, params):
        """
        Function to manage (remove from) failed_urls list when a request is returned and processed successfully.

        Inputs
        ------
        - parser_func : str
            - String denoting parser function where url failed.

        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
        """

        # wrapper contains function name and parameters that failed the last time.
        wrapper = [parser_func, params]
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        A class method used by Scrapy to create a spider.

        Parameters
        ----------
        crawler : object
            crawler to which the spider will be bound
        args : list
            arguments passed to the __init__() method
        kwargs : dict
            keyword arguments passed to the __init__() method

        Returns
        -------
        str
            spider
        """

        spider = super(AmazonProductSalePriceBSRSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
        return spider

    def parse_movement(self, params, response):
        """
        Function to parse product page and extract daily movement info (sale price, BSR, etc.) in specific format.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonProductDailyMovementItem
            - Instance of AmazonProductDailyMovementItem storing list of comments.
                - This instance is passed to AmazonProductDailyMovementToMongoPipeline.process_item
        """

        print(response.url, response.status)
        print(params)

        # with open("api_hit_log_file.log", "a") as f:
        #     f.write(
        #         "{}, {}, {}\n".format(
        #             datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        #             response.url,
        #             response.status,
        #         )
        #     )

        with open("{:%Y-%m-%d}.log".format(datetime.datetime.now()), "a") as f:
            f.write(
                "{}, {}\n".format(
                    response.url,
                    response.status,
                )
            )

        # with open("page.html", "wb") as html_file:
        #     html_file.write(response.body)

        failed = False

        if response.status != 200:
            failed = True

        item = AmazonProductDailyMovementItem()
        helper = AmazonScrapingHelper()

        # try:
        #     title = helper.get_title(response)
        # except Exception:
        #     logging.error("Exception occurred", exc_info=True)
        #     title = "NA"
        #     failed = True

        if not failed:
            try:
                sale_price = helper.get_sale_price(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                sale_price = "NA"
                failed = True

        if not failed:
            try:
                best_seller_rank = helper.get_best_seller_rank_1(response, self.time)
                if best_seller_rank["value"] == "NA":
                    best_seller_rank = helper.get_best_seller_rank_2(
                        response, self.time
                    )
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                best_seller_rank = "NA"
                failed = True

        try:
            # asin = helper.get_asin(response)
            asin = params["url"].split("/dp/")[1]
            if asin == "NA":
                failed = True
        except Exception:
            logging.error("Exception occurred", exc_info=True)
            asin = "NA"
            failed = True

        # try:
        #     product_details = helper.get_product_details_1(response)
        #     if product_details == {}:
        #         product_details = helper.get_product_details_2(response)
        #     if product_details == {} and self.cold_run:
        #         failed = True
        # except Exception:
        #     logging.error("Exception occurred", exc_info=True)
        #     product_details = "NA"
        #     failed = True

        if not failed:
            try:
                fullfilled = helper.get_fullfilled(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                fullfilled = "NA"
                failed = True

        if not failed:
            try:
                rating = helper.get_rating(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                rating = "NA"
                failed = True

        if not failed:
            try:
                total_reviews = helper.get_total_reviews(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                total_reviews = "NA"
                failed = True

        if not failed:
            try:
                availability = helper.get_availability(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                availability = "NA"
                failed = True

        if not failed:
            try:
                subscription_discount = helper.get_subscription_discount(
                    response, self.time
                )
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                subscription_discount = "NA"
                failed = True

        if not failed:
            try:
                original_price = helper.get_original_price(response, self.time)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                original_price = "NA"
                failed = True

        # if not failed:
        #     try:
        #         total_questions = helper.get_total_questions(response, self.time)
        #     except Exception:
        #         logging.error("Exception occurred", exc_info=True)
        #         total_questions = "NA"
        #         failed = True

        if not failed:
            # item["product_name"] = title
            item["product_sale_price"] = sale_price
            item["product_rating"] = rating
            item["product_total_reviews"] = total_reviews
            item["product_best_seller_rank"] = best_seller_rank
            item["product_asin"] = asin
            # item["product_details"] = product_details
            item["product_fullfilled"] = fullfilled
            item["product_availability"] = availability
            item["product_subscription_discount"] = subscription_discount
            item["product_original_price"] = original_price
            # item["product_total_questions"] = total_questions

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/BSR_F{}.html".format(asin), "w", encoding="utf-8"
                ) as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("movement", params)
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/BSR_R{}.html".format(asin), "w", encoding="utf-8"
                ) as f:
                    f.write(response.text)
            yield item

    def handle_spider_closed(self, reason):
        """
        A class method used to provide a shortcut to signals.connect() for the spider_closed signal.

        Parameters
        ----------
        reason : str
            a string which describes the reason why the spider was closed
        """

        self.crawler.stats.set_value("failed_urls", self.failed_urls)

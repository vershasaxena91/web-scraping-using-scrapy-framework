from amazon_product_scraping.items import AmazonProductCommentsItem
from amazon_product_scraping.utils.AmazonScrapingHelper import (
    AmazonCommentsScrapingHelper,
)
import logging
from scrapy import signals
from functools import partial
import datetime
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest

logger = logging.getLogger("scraper")
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)


class AmazonProductCommentsSpider(WebScrapingApiSpider):
    """
    Amazon Product Comments Spider
    --
    Spider for scraping comments from comment pages of Amazon products. e.g. (https://www.amazon.in/Philips-DuraPower-Trimmer-BT3211-15/product-reviews/B07D1HRHLV/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=1)

    Inputs
    ------
    - cold_run : Boolean
        - Set to True if 1st run.

    - failed_urls : list
        - List of URLs & functions failed in previous run.
        - Required when cold_run = False

    - count : int
        - If count > 0 : 'count' number of comments to scrape.
        - If count < 0 : scrape comments from last 'count' number of days.

    - success_counts : dict
        - Dictionary to store all counts.
            - prods_checked : will be updated based on how many products were checked for comments.
            - prods_with_new_comms : will be updated based on how many products had comments eligible for adding to DB.
            - new_comments : will be updated based on number of new comments added to DB.

    - mongo_db : str
        - MongoDB Name

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
    name = "AmazonProductCommentsSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.CommentsToMongoPipeline": 400
        }
    }

    def start_requests(self):
        """
        Function to send requests to start crawling.

        ---

        - When cold_run = True, uses urls generated in CommentsToMongoPipeline.open_spider.
        - When cold_run = False, uses urls from failed_urls.
        """

        if self.cold_run:
            # If running for the first time.
            for url in self.urls:
                self.add_to_failed("comms", {"url": url})
                yield WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_comms, {"url": url})
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "comms":
                    yield WebScrapingApiRequest(
                        url=params["url"], callback=partial(self.parse_comms, params)
                    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.count = kwargs["count"]
        self.success_counts = kwargs["success_counts"]
        self.urls = []
        self.mongo_db = kwargs["mongo_db"]

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

        spider = super(AmazonProductCommentsSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
        return spider

    def parse_comms(self, params, response):
        """
        Function to parse comments page and extract comments in specific format.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonProductCommentsItem
            - Instance of AmazonProductCommentsItem storing list of comments.
                - This instance is passed to CommentsToMongoPipeline.process_item

        - Next URL Request : Request
            - If the count is not satisfied and next page exists then, request is made to next page providing parse_comms as callback function.
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

        failed = False
        if response.status != 200:
            failed = True

        asin = params["url"].split("/product-reviews/")[1].split("/")[0]
        helper = AmazonCommentsScrapingHelper()
        item = AmazonProductCommentsItem()

        try:
            comments = helper.get_comments(response)
            # if len(comments) == 0 and self.cold_run:
            #     failed = True
        except:
            logging.error("Exception occured", exc_info=True)
            comments = []
            failed = True

        item["product_asin"] = asin
        item["product_comments"] = []

        count = 0
        if self.count >= 10:
            count = self.count - (int(params["url"].split("&pageNumber=")[1]) - 1) * 10
        else:
            count = self.count

        for comm in comments:
            if (self.count > 0 and len(item["product_comments"]) < count) or (
                self.count < 0
                and (datetime.datetime.now() - comm["date"]).days <= -self.count
            ):
                item["product_comments"].append(comm)

        if failed:
            if self.debug:
                print("**DEBUG:**/n {}".format(item))
                with open(
                    "fails/Comments_{}.html".format(asin), "w", encoding="utf-8"
                ) as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("comms", params)

            if self.count < 0 and len(comments) > 0:
                if (datetime.datetime.now() - comments[-1]["date"]).days <= -self.count:
                    # next_page_url = "{}&pageNumber={}".format(url.split("&pageNumber=")[0], 1+int(url.split("&pageNumber=")[1]))
                    # curr_url = unquote(url.split("&url=")[1])
                    # next_page_url = "{}&url={}".format(url.split("&url=")[0], quote("{}&pageNumber={}".format(curr_url.split("&pageNumber=")[0], 1+int(curr_url.split("&pageNumber=")[1])).encode('utf-8')))
                    next_page_url = "{}&pageNumber={}".format(
                        params["url"].split("&pageNumber=")[0],
                        1 + int(params["url"].split("&pageNumber=")[1]),
                    )
                    self.add_to_failed("comms", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(self.parse_comms, {"url": next_page_url})
                        # meta={
                        #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                        # },
                    )
            elif self.count > 0 and len(comments) > 0:
                if int(params["url"].split("&pageNumber=")[1]) < self.count / 10:
                    # self.count = self.count - (int(params['url'].split("&pageNumber=")[1]))*10
                    next_page_url = "{}&pageNumber={}".format(
                        params["url"].split("&pageNumber=")[0],
                        1 + int(params["url"].split("&pageNumber=")[1]),
                    )
                    self.add_to_failed("comms", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(self.parse_comms, {"url": next_page_url})
                        # meta={
                        #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                        # },
                    )
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

from amazon_product_scraping.items import NykaaProductCommentsItem
import logging
from scrapy import signals
from functools import partial
import datetime
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest

logger = logging.getLogger("scraper")
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)


class NykaaProductCommentsSpider(WebScrapingApiSpider):
    """
    A class for scrapy spider.

    Attributes
    ----------
    handle_httpstatus_all : Boolean
        True
    name : str
        spider name which identify the spider
    rotate_user_agent : Boolean
        True
    allowed_domains : list
        contains base-URLs for the allowed domains for the spider to crawl
    start_urls : list
        a list of URLs for the spider to start crawling from
    """

    debug = False
    handle_httpstatus_all = True
    name = "NykaaProductCommentsSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.NykaaCommentsToMongoPipeline": 400
        }
    }

    def start_requests(self):
        """
        This class method must return an iterable with the first Requests to crawl for this spider.

        Set our proxy port http://scraperapi:API_KEY@proxy-server.scraperapi.com:8001 as the proxy in the meta parameter.
        """

        if self.cold_run:
            for params in self.urls:
                self.add_to_failed("comms", params)
                yield WebScrapingApiRequest(
                    url=params["url"], callback=partial(self.parse_comms, params)
                )
        else:
            for func, params in self.failed_urls:
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
        self.time = datetime.datetime.strptime(kwargs["time"], "%Y-%m-%d %H:%M:%S")

    def add_to_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)

    def remove_from_failed(self, parser_func, params):
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

        spider = super(NykaaProductCommentsSpider, cls).from_crawler(
            crawler, *args, **kwargs
        )
        crawler.signals.connect(spider.handle_spider_closed, signals.spider_closed)
        return spider

    def parse_comms(self, params, response):
        """
        A class method used to parse the response for each request, extract scraped data as dicts and save failed urls in a csv file.

        Parameters
        ----------
        asin : str
            Asin of the product for which comments are being scraped

        response : object
            represents an HTTP response

        Raise
        -----
        Exception
            If not exist xpath of attributes of each request

        Returns
        -------
        list of dicts
            extract the scraped comments as a list of dicts
        """

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

        data = response.json()["response"]
        item = NykaaProductCommentsItem()

        try:
            item["product_id"] = params["url"].split("/products/")[1].split("/")[0]
            item["scraped_on"] = self.time.strftime("%Y-%m-%d %H:%M:%S")
            if len(item["product_id"]) == 0:
                failed = True
        except:
            failed = True

        try:
            comments = []
            for comm in data["reviewData"]:
                comments.append(
                    {
                        "username": comm["name"],
                        "rating": comm["rating"],
                        "title": comm["title"],
                        "date": comm["createdOn"],
                        "description": comm["description"],
                        "likes": comm["likeCount"],
                    }
                )
        except:
            logging.error("Exception occured", exc_info=True)
            comments = []
            failed = True

        item["product_comments"] = []

        count = 0
        if self.count > 20:
            count = (
                self.count
                - (int(params["url"].split("pageNo=")[1].split("&")[0]) - 1) * 20
            )
        else:
            count = self.count

        for comm in comments:
            if (self.count > 0 and len(item["product_comments"]) < count) or (
                self.count < 0 and (datetime.now() - comm["date"]).days <= -self.count
            ):
                item["product_comments"].append(comm)

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/Comments_{}_{}.html".format(item["product_id"]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("comms", params)

            if self.count < 0 and len(comments) > 0:
                if (datetime.now() - comments[-1]["date"]).days <= -self.count:
                    next_page_url = "https://www.nykaa.com/gateway-api/products/{}/reviews?pageNo={}&sort=MOST_RECENT&domain=nykaa".format(
                        prod["product_id"],
                        int(params["url"].split("pageNo=")[1].split("&")[0]) + 1,
                    )
                    self.add_to_failed("comms", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(self.parse_comms, {"url": next_page_url}),
                    )
            elif self.count > 0 and len(comments) > 0:
                if (
                    int(params["url"].split("pageNo=")[1].split("&")[0])
                    < self.count / 20
                ):
                    next_page_url = "https://www.nykaa.com/gateway-api/products/{}/reviews?pageNo={}&sort=MOST_RECENT&domain=nykaa".format(
                        prod["product_id"],
                        int(params["url"].split("pageNo=")[1].split("&")[0]) + 1,
                    )
                    self.add_to_failed("comms", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(self.parse_comms, {"url": next_page_url}),
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

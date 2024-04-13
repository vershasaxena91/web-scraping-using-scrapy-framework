from amazon_product_scraping.items import AmazonSearchCount, AmazonSearchProductList
from math import ceil
from urllib.parse import quote
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
import datetime


class AmazonSearchListSpider(WebScrapingApiSpider):
    """
    Amazon Search Listing Spider.
    --
    Spider for scraping search pages. e.g. (https://www.amazon.in/s?k=shampoos)

    Inputs
    ------
    - cold_run : Boolean
        - Set to True if 1st run.

    - start_urls : list
        - List of all the Brand pages to scrape.
        - Required when cold_run = True

    - failed_urls : list
        - List of URLs & functions failed in previous run.
        - Required when cold_run = False

    - success_counts : dict
        - Dictionary to store all counts.
            - new : will be updated based on count of new products added in product_list.
            - existing : will be updated based on count of existing products found.

    - mongo_db : str
        - MongoDB Name

    - company_client : str
        - Client Name/Code.

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
    name = "AmazonSearchListSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.NewListingProductURLToMongoPipeline": 300
        }
    }

    def start_requests(self):
        """
        Function to send requests to start crawling.

        ---

        - When cold_run = True, uses urls from start_urls.
        - When cold_run = False, uses urls from failed_urls.
        """

        if self.cold_run:
            # If running for the first time.
            for url in self.urls:
                self.add_to_failed("count", {"url": url})
                yield WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_count, {"url": url})
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "count":
                    yield WebScrapingApiRequest(
                        url=params["url"], callback=partial(self.parse_count, params)
                    )
                elif func == "list":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_product_list, params),
                    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.success_counts = kwargs["success_counts"]
        self.mongo_db = kwargs["mongo_db"]
        if self.cold_run:
            self.urls = kwargs["start_urls"]
        else:
            self.urls = []

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

    def parse_count(self, params, response):
        """
        Function to parse search page and extract count of products.
        Requests are made after calculating number of pages in search list.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonSearchCount
            - Instance of AmazonSearchCount storing count of products in search list.

        - Page-wise URL Request : Request
            - After calculating number of pages in search list, request is made to each page (atmost 50 pages) and function parse_product_list is provided as callback.
        """

        print(response.url, response.status)

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

        item = AmazonSearchCount()
        counts = response.xpath(
            '//*[@id="search"]/span/div//h1/div/div[1]/div/div/span[1]/text()'
        ).extract_first()
        print(counts)
        counts = counts.split() if isinstance(counts, str) else []
        print(counts)
        try:
            if "over" in counts:
                count = int(counts[counts.index("over") + 1].replace(",", ""))
                per_page = int(
                    counts[counts.index("of") - 1].split("-")[1].replace(",", "")
                )
            elif "of" in counts:
                count = int(counts[counts.index("of") + 1].replace(",", ""))
                per_page = int(
                    counts[counts.index("of") - 1].split("-")[1].replace(",", "")
                )
            else:
                count = int(counts[counts.index("results") - 1].replace(",", ""))
                per_page = count
        except:
            count = None
            print("No count of products found")
            failed = True

        item["count"] = count
        print("**********\n Count:", count)

        if failed:
            if self.debug:
                with open(
                    "fails/Search_List_{}.html".format(params["url"]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/Search_List_{}.html".format(params["url"]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("count", params)

            yield item
            if isinstance(count, int):
                for i in range(1, min(40, ceil(count / per_page) + 1)):
                    next_page_url = params["url"] + quote(
                        "?page={}".format(i).encode("utf-8")
                    )
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
        Function to parse search page and extract asins of products.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonSearchProductList
            - Instance of AmazonSearchProductList storing list of products.
                - This instance is passed to NewListingProductURLToMongoPipeline.process_item
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

        items = AmazonSearchProductList()
        products = []
        try:
            # # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div
            # asins = set(
            #     response.xpath(
            #         '//*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div/@data-asin'
            #     ).extract()
            # )
            asins = set(
                response.xpath(
                    '//*[@id="search"]/div[1]/div[1]/div/span[1]/div[1]/div/@data-asin'
                ).extract()
            )
            asins.discard("")
            for asin in asins:
                products.append(
                    {
                        "product_asin": asin,
                        "product_url": "https://www.amazon.in/dp/{}".format(asin),
                    }
                )

            if len(asins) == 0:
                failed = True
        except:
            failed = True

        print(len(products))

        if failed:
            yield None
        else:
            self.remove_from_failed("list", params)
            items["products"] = products
            # print("****\nItem:", items)

            yield items

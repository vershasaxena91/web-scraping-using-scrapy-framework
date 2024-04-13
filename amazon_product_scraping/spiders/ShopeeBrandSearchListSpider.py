from amazon_product_scraping.items import ShopeeSearchProductList
from math import ceil
from urllib.parse import quote
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial


class ShopeeBrandSearchListSpider(WebScrapingApiSpider):
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
    name = "ShopeeBrandSearchListSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.ShopeeNewListingProductURLToMongoPipeline": 300
        }
    }

    def start_requests(self):
        """
        This class method must return an iterable with the first Requests to crawl for this spider.

        Set our proxy port http://scraperapi:API_KEY@proxy-server.scraperapi.com:8001 as the proxy in the meta parameter.
        """

        if self.cold_run:
            for url in self.urls:
                self.add_to_failed("count", {"url": url})

                yield WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_count, {"url": url})
                )
        else:
            for func, params in self.failed_urls:
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
        self.urls = []
        self.brand_ids = kwargs["brand_ids"]

    def add_to_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)

    def remove_from_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)

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
                    "fails/Search_List_{}.json".format(params["url"]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/Search_List_{}.json".format(params["url"]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("count", params)

            yield None
            if isinstance(count, int):
                for i in range(0, min(50, ceil(count / per_page))):
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
        items = ShopeeSearchProductList()
        products = []
        try:
            for prod in data["items"]:
                print(prod)
                prod = prod["item_basic"]
                products.append(
                    {
                        "product_itemid": str(prod["itemid"]),
                        "product_shopid": str(prod["shopid"]),
                        "product_url": "https://shopee.co.id/api/v4/item/get?itemid={}&shopid={}".format(
                            prod["itemid"], prod["shopid"]
                        ),
                    }
                )

            if len(products) == 0:
                failed = True
        except:
            failed = True

        print(len(products))

        if failed:
            yield None
        else:
            self.remove_from_failed("list", params)
            items["products"] = products
            print("****\nItem:", items)

            yield items
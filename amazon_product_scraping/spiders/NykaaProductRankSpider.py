from amazon_product_scraping.items import NykaaRankItem
from math import ceil
from functools import partial
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
import datetime


class NykaaProductRankSpider(WebScrapingApiSpider):
    # Make Debug = False while deploying
    debug = False
    handle_httpstatus_all = True
    name = "NykaaProductRankSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.NykaaProductRankPipeline": 300
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.pages = kwargs["pages"]
        self.success_counts = kwargs["success_counts"]
        self.time = kwargs["time"]
        if self.cold_run:
            self.urls = kwargs["start_urls"]
        else:
            self.urls = []
        self.mongo_db = kwargs["mongo_db"]
        self.prods = kwargs["prods"]

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

        if not failed:
            counts = response.xpath('//*[@id="title"]/h1/span/text()[2]').extract()
            print(counts)
            try:
                count = int(counts[0])
                per_page = 20
                print(count, per_page)
            except:
                count = None
                print("No count of products found")
                failed = True

            print("**********\n Count:", count)

        if failed:
            if self.debug:
                with open(
                    "fails/Rank_{}.html".format("nykaa_rank_count_fail"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/Rank_{}.html".format("nykaa_rank_count_succ"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("count", params)
            yield None
            if isinstance(count, int):
                for i in range(1, min(self.pages + 1, ceil(count / per_page) + 1)):
                    next_page_url = params["url"].split("page_no=")[
                        0
                    ] + "page_no={}".format(i)
                    self.add_to_failed("list", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(
                            self.parse_product_list, {"url": next_page_url}
                        ),
                        dont_filter=True,
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

        if not failed:
            links = list(
                response.xpath(
                    '//*[@id="product-list-wrap"]/div/div/div[1]/a/@href'
                ).extract()
            )
            page = int(params["url"].split("page_no=")[1]) - 1
            counts = response.xpath('//*[@id="title"]/h1/span/text()[2]').extract()
            print(counts)
            if len(counts) == 0:
                failed = True

        if not failed:
            try:
                rank_start = ((int(params["url"].split("page_no=")[1]) - 1) * 20) + 1
                rank_end = int(params["url"].split("page_no=")[1]) * 20
                print(rank_start, rank_end)
            except:
                print("No count of products found")
                failed = True

        if failed:
            yield None
        else:
            i = 0
            product_ranks = []
            rank = rank_start
            if self.debug:
                with open(
                    "fails/Rank_nykaa_rank_list_{}.html".format(page),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            for link in links:
                i += 1
                if link != "":
                    product_id = link.split("/p/")[1].split("?")[0]
                    self.prods[product_id] = self.prods.get(
                        product_id, {"sum_rank": 0, "count": 0}
                    )
                    self.prods[product_id]["sum_rank"] += rank
                    self.prods[product_id]["count"] += 1
                    print(product_id)
                    print(self.prods[product_id])
                    product_ranks.append(
                        {
                            "product_rank": {
                                "time": self.time,
                                "value": self.prods[product_id]["sum_rank"]
                                / self.prods[product_id]["count"],
                            },
                            "product_id": product_id,
                            "product_url": "https://www.nykaa.com"
                            + link.split("/p/")[0]
                            + "/p/{}".format(product_id),
                            "add": "push"
                            if self.prods[product_id]["count"] == 1
                            else "reset",
                        }
                    )
                    rank += 1

            assert rank != rank_end, "Rank & End Rank does not match."

            self.remove_from_failed("list", params)
            items = NykaaRankItem()
            items["product_ranks"] = product_ranks
            yield items

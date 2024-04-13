from amazon_product_scraping.items import FlipkartRankItem
from math import ceil
from functools import partial
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
import datetime


class FlipkartProductRankSpider(WebScrapingApiSpider):
    # Make Debug = False while deploying
    debug = False
    handle_httpstatus_all = True
    name = "FlipkartProductRankSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.FlipkartProductRankPipeline": 300
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
                    params={
                        "render_js": 1,
                        "wait_until": "domcontentloaded",
                        "timeout": 20000,
                    },
                    callback=partial(self.parse_count, {"url": url}),
                )
        else:
            for func, params in self.failed_urls:
                if func == "count":

                    yield WebScrapingApiRequest(
                        url=params["url"],
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
                        callback=partial(self.parse_count, params),
                    )
                elif func == "list":

                    yield WebScrapingApiRequest(
                        url=params["url"],
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
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

    def hash(self, pid, marketplace):
        return pid + "--" + marketplace

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

        if not failed:
            # response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]/div/div/span/text()')
            counts = response.xpath(
                '//*[@id="container"]/div/div[3]/div/div[2]/div[1]/div/div/span/text()'
            ).extract_first()
            print(counts)
            counts = counts.split() if isinstance(counts, str) else []
            print(counts)
            try:
                if "of" in counts:
                    count = int(counts[counts.index("of") + 1].replace(",", ""))
                    per_page = int(counts[counts.index("–") + 1].replace(",", ""))
                    print(count, per_page)
                else:
                    print("**DEBUG**: ERROR {}".format(counts))
            except:
                count = None
                print("No count of products found")
                failed = True

            print("**********\n Count:", count)

        if failed:
            if self.debug:
                with open(
                    "fails/Rank_{}.html".format("flipkart_rank_count_fail"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/Rank_{}.html".format("flipkart_rank_count_succ"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("count", params)
            yield None
            if isinstance(count, int):
                for i in range(1, min(self.pages + 1, ceil(count / per_page) + 1)):
                    next_page_url = params["url"] + "&page={}".format(i)
                    self.add_to_failed("list", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
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

        if not failed:
            # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[1]/div/a[2]
            # //*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div[2]/div/a[2]

            links = list(
                response.xpath(
                    '//*[@id="container"]/div/div[3]/div/div[2]/div/div/div/div/a/@href'
                ).extract()
            )

            page = int(params["url"].split("&page=")[1].split("&")[0]) - 1

            # response.xpath('//*[@id="container"]/div/div[3]/div[1]/div[2]/div[1]/div/div/span/text()')
            counts = response.xpath(
                '//*[@id="container"]/div/div[3]/div/div[2]/div[1]/div/div/span/text()'
            ).extract_first()

            print(counts)
            counts = counts.split() if isinstance(counts, str) else []
            print(counts)
            if len(counts) == 0:
                failed = True

        if not failed:
            try:
                if "–" in counts:
                    rank_start = int(counts[counts.index("–") - 1].replace(",", ""))
                    rank_end = int(counts[counts.index("–") + 1].replace(",", ""))
                    print(rank_start, rank_end)
                else:
                    print("**DEBUG**: ERROR {}".format(counts))
                    failed = True
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
                    "fails/Rank_flipkart_rank_list_{}.html".format(page),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            for link in links:
                i += 1
                if link != "":
                    static = link.split("?")[0]
                    pid = link.split("pid=")[1].split("&")[0]
                    try:
                        lid = link.split("lid=")[1].split("&")[0]
                    except:
                        lid = ""
                    try:
                        marketplace = link.split("marketplace=")[1].split("&")[0]
                    except:
                        marketplace = "FLIPKART"
                    key = self.hash(pid, marketplace)
                    self.prods[key] = self.prods.get(key, {"sum_rank": 0, "count": 0})
                    self.prods[key]["sum_rank"] += rank
                    self.prods[key]["count"] += 1
                    print(key)
                    print(self.prods[key])
                    product_ranks.append(
                        {
                            "product_rank": {
                                "time": self.time,
                                "value": self.prods[key]["sum_rank"]
                                / self.prods[key]["count"],
                            },
                            "product_pid": pid,
                            "product_lid": lid,
                            "marketplace": marketplace,
                            "product_url": "https://www.flipkart.com{}?pid={}&lid={}&marketplace={}".format(
                                static, pid, lid, marketplace
                            ),
                            "add": "push" if self.prods[key]["count"] == 1 else "reset",
                        }
                    )
                    rank += 1

            assert rank != rank_end, "Rank & End Rank does not match."

            self.remove_from_failed("list", params)
            items = FlipkartRankItem()
            items["product_ranks"] = product_ranks
            yield items

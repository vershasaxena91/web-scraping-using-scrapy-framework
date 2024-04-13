from amazon_product_scraping.items import (
    FlipkartShareOfSearchItem,
    FlipkartShareOfSearchRanksItem,
)
from math import ceil
from functools import partial
from amazon_product_scraping.utils.FlipkartScrapingHelper import FlipkartAllInfoHelper
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from urllib.parse import unquote
import datetime


class FlipkartShareOfSearchSpider(WebScrapingApiSpider):

    # Make Debug = False while deploying
    debug = False
    handle_httpstatus_all = True
    name = "FlipkartShareOfSearchSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.FlipkartShareOfSearchPipeline": 300
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
                elif func == "data":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
                        callback=partial(self.parse_product_data, params),
                    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_urls = kwargs["failed_urls"]
        self.cold_run = kwargs["cold_run"]
        self.keywords = kwargs["keywords"]
        self.pages = kwargs["pages"]
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

        counts = (
            response.xpath(
                '//*[@id="container"]/div/div[3]/div/div[2]/div[1]/div/div/span/text()'
            ).extract_first()
            or response.xpath(
                '//*[@id="container"]/div/div[3]/div[1]/div[2]/div[2]/div/div/span[1]/text()'
            ).extract_first()
        )

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
                failed = True
        except:
            count = None
            print("No count of products found")
            failed = True

        if not failed:
            print("**********\n Count:", count)

        if failed:
            if self.debug:
                with open(
                    "fails/SOS_{}.html".format("flipkart_count_fail"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/SOS_{}.html".format("flipkart_count_succ"),
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

        items = FlipkartShareOfSearchRanksItem()
        keyword = unquote(params["url"].split("?q=")[1].split("&")[0])
        page = int(params["url"].split("&page=")[1].split("&")[0]) - 1
        rank = page * 40

        if not failed:
            links = list(
                response.xpath(
                    '//*[@id="container"]/div/div[3]/div/div[2]/div/div/div/div/a/@href'
                ).extract()
            )
            if len(links) == 0:
                failed = True

        if failed:
            if self.debug:
                with open(
                    "fails/SOS_flipkart_list_fail_{}.html".format(page),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("list", params)

            i = 0
            product_ranks = []
            if self.debug:
                with open(
                    "fails/SOS_flipkart_list_succ_{}.html".format(page),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            for link in links:
                i += 1
                if link != "":
                    rank += 1
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
                    product_ranks.append(
                        {
                            "product_pid": pid,
                            "marketplace": marketplace,
                            "keyword": keyword,
                            "product_rank": rank,
                        }
                    )
                    # print(rank, isSponsored)
                    if not self.present_check_func({"product_pid": pid}):

                        url = "https://www.flipkart.com{}?pid={}&marketplace={}".format(
                            static, pid, "FLIPKART"
                        )
                        # if isSponsored:
                        #     print('Sponsored', i, rank)
                        wrapper = {"url": url, "marketplace": marketplace, "lid": lid}
                        if self.add_to_failed("data", wrapper):
                            yield WebScrapingApiRequest(
                                url=url,
                                params={
                                    "render_js": 1,
                                    "wait_until": "domcontentloaded",
                                    "timeout": 20000,
                                },
                                callback=partial(self.parse_product_data, wrapper),
                                # meta={
                                #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                                # }
                            )
            items["product_ranks"] = product_ranks
            yield items

    def parse_product_data(self, params, response):
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

        pid = params["url"].split("pid=")[1].split("&")[0]
        lid = params["lid"]
        marketplace = params["marketplace"]
        item = FlipkartShareOfSearchItem()

        if not failed:
            helper = FlipkartAllInfoHelper(response, self.time)
            try:
                info = helper.getInfo()
                move = helper.getMovement()
                for k, v in move.items():
                    move[k] = v["value"]
            except:
                failed = True
                info = dict()
                move = dict()

        if not failed:
            item["product_name"] = info["title"]
            item["product_brand"] = info["brand"]
            item["product_original_price"] = info["original_price"]
            item["product_category"] = info["categories"]
            item["product_highlights"] = info["highlights"]
            item["product_services"] = info["services"]
            item["product_details"] = info["specifications"]
            item["product_pid"] = pid
            item["product_lid"] = lid
            item["marketplace"] = marketplace
            item["product_description"] = info["description"]
            item["product_url"] = params["url"]
            item["scraped_on"] = self.time

            item["product_sale_price"] = move["sale_price"]
            item["product_rating"] = move["rating"]
            item["product_total_ratings"] = move["num_ratings"]
            item["product_total_reviews"] = move["num_reviews"]
            item["product_availability"] = move["availability"]
            item["product_assured"] = move["assured"]

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/SOS_flipkart_data_fail_{}.html".format(pid),
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
                    "fails/SOS_flipkart_data_succ_{}.html".format(pid),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)

            yield item
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[2]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[3]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[4]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span

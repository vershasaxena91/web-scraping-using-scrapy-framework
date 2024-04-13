from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
from math import ceil
from amazon_product_scraping.items import FlipkartSearchProductList
import datetime


class FlipkartSearchListSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "FlipkartSearchListSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.FlipkartNewListingProductURLToMongoPipeline": 300
        }
    }

    def start_requests(self):
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
        self.success_counts = kwargs["success_counts"]
        self.mongo_db = kwargs["mongo_db"]
        if self.cold_run:
            self.urls = kwargs["start_urls"]
        else:
            self.urls = []

    def add_to_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)

    def remove_from_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)

    def parse_count(self, params, response):

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
            # //*[@id="container"]/div/div[3]/div/div[2]/div[1]/div/div/span/text()[1]
            counts = response.xpath(
                '//*[@id="container"]/div/div[3]/div/div[2]/div[1]/div/div/span/text()'
            ).extract_first()
            print(counts)
            # Showing 1 – 40 of 355 results for "
            counts = counts.split() if isinstance(counts, str) else []
            print(counts)
            if len(counts) == 0:
                failed = True

        if not failed:
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

        if failed:
            if self.debug:
                with open(
                    "fails/Search_List_{}.html".format("flipkart_seach_count_fail"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None

        else:
            if self.debug:
                with open(
                    "fails/Search_List_{}.html".format("flipkart_search_count_succ"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("count", params)

            if isinstance(count, int):
                for i in range(1, min(5, ceil(count / per_page) + 1)):
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
                    )

    def parse_product_list(self, params, response):
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

        page = params["url"].split("&page=")[1].split("&")[0]
        if not failed:
            items = FlipkartSearchProductList()
            products = []
            try:
                # //*[@id="container"]/div/div[3]/div/div[2]/div[2]/div/div[1]/div/a[2]
                links = set(
                    response.xpath(
                        '//*[@id="container"]/div/div[3]/div/div[2]/div/div/div/div/a/@href'
                    ).extract()
                )
                links.discard("")
                for link in links:
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
                    products.append(
                        {
                            "product_pid": pid,
                            "product_lid": lid,
                            "marketplace": marketplace,
                            "product_url": "https://www.flipkart.com{}?pid={}&lid={}".format(
                                static, pid, lid
                            ),
                        }
                    )

                if len(products) == 0:
                    failed = True
            except:
                failed = True

        if failed:
            if self.debug:
                with open(
                    "fails/Search_List_{}_{}.html".format(
                        "flipkart_search_list_fail", page
                    ),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/Search_List_{}_{}.html".format(
                        "flipkart_search_list_succ", page
                    ),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            self.remove_from_failed("list", params)
            items["products"] = products
            # print("****\nItem:", items)

            yield items

# from datetime import datetime
from webscrapingapi_scrapy_sdk import WebScrapingApiRequest, WebScrapingApiSpider
from functools import partial
from amazon_product_scraping.utils.FlipkartScrapingHelper import (
    FlipkartCommentsScrapingHelper,
)
from amazon_product_scraping.items import FlipkartProductCommentsItem
import datetime


class FlipkartProductCommentsSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "FlipkartProductCommentsSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.FlipkartCommentsToMongoPipeline": 300
        }
    }

    def start_requests(self):
        if self.cold_run:
            for params in self.urls:
                self.add_to_failed("comms", params)
                yield WebScrapingApiRequest(
                    url=params["url"],
                    params={
                        "render_js": 1,
                        "wait_until": "domcontentloaded",
                        "timeout": 20000,
                    },
                    callback=partial(self.parse_comms, params),
                )
        else:
            for func, params in self.failed_urls:
                if func == "comms":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
                        callback=partial(self.parse_comms, params),
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

    def parse_comms(self, params, response):

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
        marketplace = params["url"].split("marketplace=")[1].split("&")[0]
        helper = FlipkartCommentsScrapingHelper()
        item = FlipkartProductCommentsItem()

        if not failed:
            try:
                comments = helper.get_comments(response, self.time)
                if len(comments) == 0 and self.cold_run:
                    failed = True
            except:
                comments = []
                failed = True

            item["product_pid"] = pid
            item["marketplace"] = marketplace
            item["scraped_on"] = self.time.strftime("%Y-%m-%d %H:%M:%S")
            item["product_comments"] = []

            count = 0
            if self.count >= 10:
                count = self.count - (int(params["url"].split("&page=")[1]) - 1) * 10
            else:
                count = self.count

            for comm in comments:
                if (self.count > 0 and len(item["product_comments"]) < count) or (
                    self.count < 0
                    and (datetime.now() - comm["date"]).days <= -self.count
                ):
                    item["product_comments"].append(comm)

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/Comments_flipkart_fail_{}.html".format(pid),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
            yield None

        else:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open(
                    "fails/Comments_flipkart_succ_{}.html".format(pid),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)

            self.remove_from_failed("comms", params)
            yield item

            if self.count < 0 and len(comments) > 0:
                if (self.time - comments[-1]["date"]).days <= -self.count:
                    next_page_url = "{}&page={}".format(
                        params["url"].split("&page=")[0],
                        1 + int(params["url"].split("&page=")[1]),
                    )
                    params["url"] = next_page_url
                    self.add_to_failed("comms", params)
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
                        callback=partial(self.parse_comms, params),
                    )
            elif self.count > 0 and len(comments) > 0:
                if int(params["url"].split("&page=")[1]) < self.count / 10:
                    next_page_url = "{}&page={}".format(
                        params["url"].split("&page=")[0],
                        1 + int(params["url"].split("&page=")[1]),
                    )
                    params["url"] = next_page_url
                    self.add_to_failed("comms", params)
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
                        callback=partial(self.parse_comms, params),
                    )

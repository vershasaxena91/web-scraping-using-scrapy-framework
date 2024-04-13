from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
from amazon_product_scraping.utils.FlipkartScrapingHelper import FlipkartAllInfoHelper
from amazon_product_scraping.items import FlipkartProductInfoItem
import datetime


class FlipkartProductInfoSpider(WebScrapingApiSpider):

    debug = False
    handle_httpstatus_all = True
    name = "FlipkartProductInfoSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.FlipkartProductInfoToMongoPipeline": 300
        }
    }

    def start_requests(self):
        if self.cold_run:
            for params in self.urls:
                self.add_to_failed("info", params)

                yield WebScrapingApiRequest(
                    url=params["url"],
                    params={
                        "render_js": 1,
                        "wait_until": "domcontentloaded",
                        "timeout": 20000,
                    },
                    callback=partial(self.parse_info, params),
                )
        else:
            for func, params in self.failed_urls:
                if func == "info":

                    yield WebScrapingApiRequest(
                        url=params["url"],
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "timeout": 20000,
                        },
                        callback=partial(self.parse_info, params),
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
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)

    def remove_from_failed(self, parser_func, params):
        wrapper = [parser_func, params]
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)

    def parse_info(self, params, response):
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

        item = FlipkartProductInfoItem()

        try:
            # https://www.flipkart.com/dove-intense-repair-shampoo/p/itmf3xfyygchayd8?pid=SMPDG2X8P8HGAXTA
            pid = params["url"].split("pid=")[1].split("&")[0]
            if pid is None or pid == "":
                failed = True
        except:
            pid = "NA"
            failed = True

        if not failed:
            try:
                lid = params["url"].split("lid=")[1].split("&")[0]
            except:
                lid = "NA"

        # if not failed:
        #     try:
        #         marketplace = params["url"].split("marketplace=")[1].split("&")[0]
        #     except:
        #         marketplace = "NA"
        if not failed:
            helper = FlipkartAllInfoHelper(response, self.time)
            try:
                info = helper.getInfo()
                if not (
                    (isinstance(info["title"], str) and len(info["title"]) > 0)
                    and (isinstance(info["brand"], str) and len(info["brand"]) > 0)
                ):
                    failed = True
            except:
                failed = True
                info = dict()

        if not failed:
            item["product_name"] = info["title"]
            item["product_brand"] = info["brand"]  # info["specifications"]["Brand"]
            item["product_original_price"] = info["original_price"]
            item["product_highlights"] = info["highlights"]
            item["product_services"] = info["services"]
            item["product_details"] = info["specifications"]
            item["product_pid"] = pid
            item["product_lid"] = lid
            item["marketplace"] = params["marketplace"]
            item["product_description"] = info["description"]
            item["product_category"] = info["categories"]
            item["product_url"] = params["url"]
            item["scraped_on"] = self.time

        if failed:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
                with open("fails/{}.html".format(pid), "w", encoding="utf-8") as f:
                    f.write(response.text)
            yield None

        else:
            if self.debug:
                print("**DEBUG:** {}\n {}".format(failed, str(item).encode("utf-8")))
            self.remove_from_failed("info", params)
            yield item

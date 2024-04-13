from functools import partial
import datetime
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from amazon_product_scraping.items import AmazonQuestionsItem
from amazon_product_scraping.utils.AmazonScrapingHelper import AmazonQAScrapingHelper


class AmazonProductQuestionsSpider(WebScrapingApiSpider):
    """
    Amazon Product Questions Spider
    --
    Spider for scraping Q&As from questions pages of Amazon products. e.g. (https://www.amazon.in/ask/questions/asin/B07D1HRHLV/1/ref=ask_dp_iaw_ql_hza?isAnswered=true)

    Inputs
    ------
    - cold_run : Boolean
        - Set to True if 1st run.

    - failed_urls : list
        - List of URLs & functions failed in previous run.
        - Required when cold_run = False

    - count : int
        - If count > 0 : 'count' number of questions to scrape.
        - If count < 0 : scrape questions from last 'count' number of days.

    - success_counts : dict
        - Dictionary to store all counts.
            - prods_checked : will be updated based on how many products were checked for questions.
            - prods_with_new_QAs : will be updated based on how many products had questions eligible for adding to DB.
            - new_QAs : will be updated based on number of new questions added to DB.

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
    name = "AmazonProductQuestionsSpider"
    rotate_user_agent = True

    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.QuestionsToMongoPipeline": 400
        }
    }

    def start_requests(self):
        """
        Function to send requests to start crawling.

        ---

        - When cold_run = True, uses urls generated in QuestionsToMongoPipeline.open_spider.
        - When cold_run = False, uses urls from failed_urls.
        """

        if self.cold_run:
            # If running for the first time.
            for url in self.urls:
                self.add_to_failed("questions", {"url": url})
                yield WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_questions, {"url": url})
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "questions":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_questions, params),
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

    def parse_questions(self, params, response):
        """
        Function to parse questions page and extract questions & answers in specific format.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonQuestionsItem
            - Instance of AmazonQuestionsItem storing list of questions.
                - This instance is passed to QuestionsToMongoPipeline.process_item

        - Next URL Request : Request
            - If the count is not satisfied and next page exists then, request is made to next page providing parse_questions as callback function.
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

        asin = params["url"].split("/asin/")[1].split("/")[0]
        helper = AmazonQAScrapingHelper()
        item = AmazonQuestionsItem()

        try:
            QAs = helper.get_QAs(response)
        except:
            QAs = []
            failed = True
        print("QA", str(QAs).encode("utf-8"))
        print(asin)
        item["product_asin"] = asin
        item["product_QAs"] = []

        count = 0
        if self.count >= 10:
            count = (
                self.count - (int(params["url"].split("?")[0].split("/")[-1]) - 1) * 10
            )
        else:
            count = self.count

        for qa in QAs:
            if (self.count > 0 and len(item["product_QAs"]) < count) or (
                self.count < 0
                and (datetime.datetime.now() - qa["date"]).days <= -self.count
            ):
                item["product_QAs"].append(qa)

        if failed:
            if self.debug:
                print("**DEBUG:**\n {}".format(str(item).encode("utf-8")))
                with open("fails/QAs_{}.html".format(asin), "w", encoding="utf-8") as f:
                    f.write(response.text)
            yield None
        else:
            self.remove_from_failed("questions", params)

            if self.count < 0 and len(QAs) == 10:
                if (datetime.datetime.now() - QAs[-1]["date"]).days <= -self.count:
                    next_page_url = "{}/{}?{}".format(
                        "/".join(params["url"].split("?")[0].split("/")[:-1]),
                        1 + int(params["url"].split("?")[0].split("/")[-1]),
                        params["url"].split("?")[1],
                    )
                    self.add_to_failed("questions", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(self.parse_questions, {"url": next_page_url})
                        # meta={
                        #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                        # },
                    )
            elif self.count > 0 and len(QAs) == 10:
                if int(params["url"].split("?")[0].split("/")[-1]) < self.count / 10:
                    next_page_url = "{}/{}?{}".format(
                        "/".join(params["url"].split("?")[0].split("/")[:-1]),
                        1 + int(params["url"].split("?")[0].split("/")[-1]),
                        params["url"].split("?")[1],
                    )
                    self.add_to_failed("questions", {"url": next_page_url})
                    yield WebScrapingApiRequest(
                        url=next_page_url,
                        callback=partial(self.parse_questions, {"url": next_page_url})
                        # meta={
                        #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                        # },
                    )
            yield item

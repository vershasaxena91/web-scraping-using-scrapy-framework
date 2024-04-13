from amazon_product_scraping.items import AmazonSearchProductList
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
import datetime


class AmazonBrandSearchListSpider(WebScrapingApiSpider):
    """
    Amazon Brand Search Listing Spider.
    --
    Spider for scraping brand pages. e.g. (https://www.amazon.in/stores/page/27503F6D-DEF1-4407-8223-C753B9654A6B?ingress=2&visitId=a20fa23a-dec7-4e33-815b-3978be44f97e&ref_=ast_bln)

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
    name = "AmazonBrandSearchListSpider"
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
                self.add_to_failed("brand_list", {"url": url})
                yield WebScrapingApiRequest(
                    url=url,
                    params={
                        "render_js": 1,
                        "wait_until": "domcontentloaded",
                        "wait_for": 5000,
                    },
                    callback=partial(self.parse_brand_list, {"url": url}),
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "brand_list":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        params={
                            "render_js": 1,
                            "wait_until": "domcontentloaded",
                            "wait_for": 5000,
                        },
                        callback=partial(self.parse_brand_list, params),
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
        if self.debug:
            print("Adding: {}".format(wrapper))
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

    def parse_brand_list(self, params, response):
        """
        Function to parse brand page and extract asins of products.

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

        # with open("page.html", "wb") as html_file:
        #     html_file.write(response.body)

        failed = False
        if response.status != 200:
            failed = True

        item = AmazonSearchProductList()
        if not failed:
            # //*[@id="a-page"]/div[2]/div
            # links_xpath = response.xpath('//*[@id="ekc875jqle"]/div/div//div//li//a/@href').extract()
            links_xpath = (
                response.xpath(
                    '//div[@id="a-page"]/div[2]/div//div/div//div//li//a[@class="style__overlay__2qYgu ProductGridItem__overlay__1ncmn"]/@href'
                ).extract()
                or response.xpath(
                    '//div[@class="stores-page"]//a[@class="ProductShowcase__title__3eXnB"]/@href'
                ).extract()
            )

            if len(links_xpath) == 0:
                failed = True

            links = [i for i in links_xpath if "/dp/" in i]
            # asins = [(i.split("/dp/")[1]).split("/")[0] for i in links]
            # asins = [i.split("/dp/")[1].split("/")[0].split('?')[0] for i in links]
            asins = []
            for i in links:
                try:
                    asins.append(i.split("/dp/")[1].split("/")[0].split("?")[0])
                except:
                    pass
            if len(asins) == 0:
                failed = True

        if not failed:
            products = []
            for asin in asins:
                products.append(
                    {
                        "product_asin": asin,
                        "product_url": "https://www.amazon.in/dp/{}".format(asin),
                    }
                )

            print(len(products))
            item["products"] = products

        if failed:
            yield None
            if self.debug:
                print("**DEBUG:**/n {}".format(str(item).encode("utf-8")))
                with open(
                    "fails/Brand_Search_{}.html".format(params["url"].split("/")[-1]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
        else:
            # if self.debug:
            #     print("**DEBUG:**/n {}".format(item))
            #     with open('fails/100_BSR_{}.html'.format(params['url'].split('/')[-1]), 'w', encoding='utf-8') as f:
            #         f.write(response.text)
            self.remove_from_failed("brand_list", params)
            yield item

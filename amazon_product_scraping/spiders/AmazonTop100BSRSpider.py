from amazon_product_scraping.items import AmazonSearchProductList
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
from functools import partial
import datetime


class AmazonTop100BSRSpider(WebScrapingApiSpider):
    """
    Amazon Top 100 BSR Listing Spider.
    --
    Spider for scraping top 100 BSR pages. e.g. (https://www.amazon.in/gp/bestsellers/beauty/ref=pd_zg_ts_beauty) (https://www.amazon.in/gp/bestsellers/beauty/ref=zg_bs_pg_2?ie=UTF8&pg=2)

    Inputs
    ------
    - cold_run : Boolean
        - Set to True if 1st run.

    - start_urls : list
        - List of all the BSR pages to scrape.
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
    name = "AmazonTop100BSRSpider"
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
                self.add_to_failed("bsr_list", {"url": url})
                yield WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_bsr_list, {"url": url})
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "bsr_list":
                    yield WebScrapingApiRequest(
                        url=params["url"], callback=partial(self.parse_bsr_list, params)
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

    def parse_bsr_list(self, params, response):
        """
        Function to parse BSR pages and extract asins of products.

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

        item = AmazonSearchProductList()
        # links_xpath = response.xpath('//div[@class="p13n-desktop-grid"]/@data-client-recs-list').extract()
        # import json
        # jsonStr = links_xpath[0]
        # jsonList = json.loads(jsonStr)
        # asin = []
        # for i in jsonList:
        #     asin.append(i['id'])
        #     print(i['id'])
        # links_xpath = response.xpath('//*[@id="p13n-asin-index-49"]/div[2]/div/a[1]/@href').extract()
        links_xpath = response.xpath('//a[@tabindex="-1"]/@href').extract()
        # links_xpath = response.xpath('//div[@class="zg-grid-general-faceout"]').xpath('//a[@class="a-link-normal"]/@tabindex/@href').extract()
        # print(len(asin))
        print(len(links_xpath))

        if len(links_xpath) == 0:
            failed = True
        try:
            links = [i for i in links_xpath if "/dp/" in i]
            # asins = [(i.split("/dp/")[1]).split("/")[0] for i in links]
            asins = [(i.split("/dp/")[1]).split("/")[0].split("?")[0] for i in links]
        except:
            failed = True
        # bsr_xpath = response.xpath(
        #     '//div[@id="zg-center-div"]//span//span//text()'
        # ).extract()
        # bsr = [i + " in Shampoos (Beauty)" for i in bsr_xpath if "#" in i]

        # item["links"] = links
        # item["asin"] = asin
        # item["bsr"] = bsr

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
                print("**DEBUG:**/n {}".format(item))
                with open(
                    "fails/100_BSR_{}.html".format(params["url"].split("/")[-1]),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(response.text)
        else:
            # if self.debug:
            #     print("**DEBUG:**/n {}".format(item))
            #     with open('fails/100_BSR_{}.html'.format(params['url'].split('/')[-1]), 'w', encoding='utf-8') as f:
            #         f.write(response.text)
            self.remove_from_failed("bsr_list", params)
            yield item

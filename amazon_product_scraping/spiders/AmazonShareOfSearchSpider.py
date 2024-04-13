from amazon_product_scraping.items import (
    AmazonShareOfSearchItem,
    AmazonSearchCount,
    AmazonShareOfSearchRanksItem,
)
from math import ceil
from functools import partial
from amazon_product_scraping.utils.AmazonScrapingHelper import AmazonScrapingHelper
from webscrapingapi_scrapy_sdk import WebScrapingApiSpider, WebScrapingApiRequest
import datetime


class AmazonShareOfSearchSpider(WebScrapingApiSpider):
    """
    Amazon Share of Search Spider.
    --
    Spider for scraping Share of Search (SOS) data (Search List + Product Data (Info + Daily Move data for 1 day)) given a keyword. e.g. (https://www.amazon.in/s?k=shampoos).

    Inputs
    ------
    - cold_run : Boolean
        - Set to True if 1st run.

    - failed_urls : list
        - List of URLs & functions failed in previous run.
        - Required when cold_run = False

    - success_counts : dict
        - Dictionary to store all counts.
            - added : will be updated based on count of new products data added in sos_data.
            - ranked : will be updated based on count of products found in keyword search list.

    - keywords : list
        - List of keywords for SOS.

    - pages : int
        - Number of pages to look for in SOS.

    - time : str
        - Timestamp

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

    # Make Debug = False while deploying
    debug = False
    handle_httpstatus_all = True
    name = "AmazonShareOfSearchSpider"
    rotate_user_agent = True
    custom_settings = {
        "ITEM_PIPELINES": {
            "amazon_product_scraping.pipelines.ShareOfSearchPipeline": 300
        }
    }

    def start_requests(self):
        """
        Function to send requests to start crawling.

        ---

        - When cold_run = True, uses urls generated in ShareOfSearchPipeline.open_spider from keywords.
        - When cold_run = False, uses urls from failed_urls.
        """

        if self.cold_run:
            for url in self.urls:
                # If running for the first time.
                self.add_to_failed("count", {"url": url})

                yield WebScrapingApiRequest(
                    url=url, callback=partial(self.parse_count, {"url": url})
                )
        else:
            # If re-running after failure.
            for func, params in self.failed_urls:
                # Select func based on wrapper and pass same params.
                if func == "count":
                    yield WebScrapingApiRequest(
                        url=params["url"], callback=partial(self.parse_count, params)
                    )
                elif func == "list":
                    yield WebScrapingApiRequest(
                        url=params["url"],
                        callback=partial(self.parse_product_list, params),
                    )
                elif func == "data":
                    yield WebScrapingApiRequest(
                        url=params["url"],
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
        """
        Function to manage (add to) failed_urls list.

        Inputs
        ------
        - parser_func : str
            - String denoting parser function where url failed.

        - params : dict
            - Required parameters to facilitate identification of url in case of failure.

        Returns
        -------
        - not_present : Boolean
            - True : If wrapper is not present in self.failed_urls. Also adds wrapper to self.failed_urls.
            - False : If wrapper is already present in self.failed_urls.
        """

        # wrapper contains function name and parameters that failed the last time.
        wrapper = [parser_func, params]
        if wrapper not in self.failed_urls:
            self.failed_urls.append(wrapper)
            return True
        else:
            return False

    def remove_from_failed(self, parser_func, params):
        """
        Function to manage (remove from) failed_urls list when a request is returned and processed successfully.

        Inputs
        ------
        - parser_func : str
            - String denoting parser function where url failed.

        - params : dict
            - Required parameters to facilitate identification of url in case of failure.

        Returns
        -------
        - present : Boolean
            - True : If wrapper is present in self.failed_urls. Also removes wrapper from self.failed_urls.
            - False : If wrapper is not present in self.failed_urls.
        """

        # wrapper contains function name and parameters that failed the last time.
        wrapper = [parser_func, params]
        if self.debug:
            print("Removing", wrapper)
        if wrapper in self.failed_urls:
            self.failed_urls.remove(wrapper)
            return True
        else:
            return False

    def parse_count(self, params, response):
        """
        Function to parse search page and extract count of products.
        Requests are made after calculating number of pages in keyword search list.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - Page-wise URL Request : Request
            - After calculating number of pages in search list, request is made to each page (atmost self.pages number of pages) and function parse_product_list is provided as callback.
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

        if not failed:
            counts = response.xpath(
                '//*[@id="search"]/span/div//h1/div/div[1]/div/div/span[1]/text()'
            ).extract_first()
            print(counts)
            counts = counts.split() if isinstance(counts, str) else []
            print(counts)
            try:
                if "over" in counts:
                    count = int(counts[counts.index("over") + 1].replace(",", ""))
                    per_page = int(
                        counts[counts.index("of") - 1].split("-")[1].replace(",", "")
                    )
                elif "of" in counts:
                    count = int(counts[counts.index("of") + 1].replace(",", ""))
                    per_page = int(
                        counts[counts.index("of") - 1].split("-")[1].replace(",", "")
                    )
                else:
                    count = int(counts[counts.index("results") - 1].replace(",", ""))
                    per_page = count
            except:
                count = None
                print("No count of products found")
                failed = True

        print("**********\n Count:", count)

        if failed:
            if self.debug:
                with open(
                    "fails/OP_{}.html".format("temp_count_fail"), "w", encoding="utf-8"
                ) as f:
                    f.write(response.text)
            yield None
        else:
            if self.debug:
                with open(
                    "fails/OP_{}.html".format("temp_count_succ"), "w", encoding="utf-8"
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
                        callback=partial(
                            self.parse_product_list, {"url": next_page_url}
                        ),
                        # meta={
                        #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                        # }
                    )

    def parse_product_list(self, params, response):
        """
        Function to parse search page and extract rank & asins of products.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonShareOfSearchRanksItem
            - Instance of AmazonShareOfSearchRanksItem storing list of products asins and ranks.
                - This instance is passed to ShareOfSearchPipeline.process_item

        - Product Data URL Request : Request
            - If the product data is neither already present in database nor already requested then, request is made to scrape product data (www.amazon.in/dp/{ASIN}) providing parse_product_data as callback function.
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

        items = AmazonShareOfSearchRanksItem()
        keyword = " ".join(params["url"].split("?k=")[1].split("&")[0].split("+"))
        page = int(params["url"].split("&page=")[1].split("&")[0]) - 1
        rank = page * 60

        if not failed:
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div
            # asins = response.xpath(
            #     '//*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div/@data-asin'
            # ).extract()
            asins = response.xpath(
                '//*[@id="search"]/div[1]/div[1]/div/span[1]/div[1]/div/@data-asin'
            ).extract()
            if len(asins) == 0:
                failed = True

        if failed:
            yield None
        else:
            self.remove_from_failed("list", params)

            i = 0
            product_ranks = []
            if self.debug:
                with open(
                    "fails/havells/OP_{}.html".format(page), "w", encoding="utf-8"
                ) as f:
                    f.write(response.text)
            for asin in asins:
                i += 1
                if asin != "":
                    # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[2]/div/ div /div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
                    sponsored_text = response.xpath(
                        '//*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[{}]/div//div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span/text()'.format(
                            i
                        )
                    ).extract()
                    isSponsored = True if len(sponsored_text) > 0 else False
                    rank += 1
                    # print(rank, isSponsored)
                    """
                    item['product_asin'] = asin
                    item['product_title'] = title
                    item['product_brand'] = brand
                    item['keyword'] = params['keyword']
                    item['product_rank'] = params['rank']
                    item['product_sale_price'] = sale_price
                    item['product_original_price'] = original_price
                    item['product_fullfilled'] = fullfilled
                    item['sponsored'] = params['sponsored']
                    """
                    product_ranks.append(
                        {
                            "product_asin": asin,
                            "keyword": keyword,
                            "product_rank": rank,
                            "sponsored": isSponsored,
                        }
                    )

                    if not self.present_check_func({"product_asin": asin}):
                        url = "https://www.amazon.in/dp/{}".format(asin)
                        # if isSponsored:
                        #     print('Sponsored', i, rank)
                        wrapper = {"url": url}

                        if self.add_to_failed("data", wrapper):
                            yield WebScrapingApiRequest(
                                url=url,
                                callback=partial(self.parse_product_data, wrapper),
                                # meta={
                                #     "proxy": "http://scraperapi:1ee5ce80f3bbdbad4407afda1384b61e@proxy-server.scraperapi.com:8001"
                                # }
                            )
            items["product_ranks"] = product_ranks
            yield items

    def parse_product_data(self, params, response):
        """
        Function to parse product page and extract product data in specific format.

        Inputs
        ------
        - params : dict
            - Required parameters to facilitate identification of url in case of failure.
            \+ Optional parameters to pass some data.

        - response : object
            - HTTP Response from request.

        Yields
        ------
        - item : AmazonShareOfSearchItem
            - Instance of AmazonShareOfSearchItem storing product data.
                - This instance is passed to ShareOfSearchPipeline.process_item
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

        asin = params["url"].split("/dp/")[1].split("/")[0]
        item = AmazonShareOfSearchItem()
        helper = AmazonScrapingHelper()

        if not failed:
            if self.debug:
                print("title")
            try:
                title = helper.get_title(response)
            except Exception:
                title = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("brand")
            try:
                brand = helper.get_brand(response)
            except Exception:
                brand = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("offers")
            try:
                offers = helper.get_offers(response)
            except Exception:
                offers = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("orig_price")
            try:
                original_price = helper.get_original_price(response, self.time)["value"]
            except Exception:
                original_price = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("category")
            try:
                category = helper.get_category(response)
            except Exception:
                category = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("icons")
            try:
                icons = helper.get_icons(response)
            except Exception:
                icons = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("details")
            try:
                product_details = helper.get_product_details_1(response, self.time)
                if product_details == {}:
                    product_details = helper.get_product_details_2(response, self.time)
                if product_details == {} and self.cold_run:
                    failed = True
            except Exception:
                product_details = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("imp_info")
            try:
                important_information = helper.get_important_information(response)
            except Exception:
                important_information = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("description")
            try:
                product_description = helper.get_product_description(response)
            except Exception:
                product_description = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("bought_together")
            try:
                bought_together = helper.get_bought_together(response)
            except Exception:
                bought_together = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("variations")
            try:
                variations = helper.get_variations(response)
            except Exception:
                variations = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("Sale Price")
            try:
                sale_price = helper.get_sale_price(response, self.time)["value"]
            except Exception:
                sale_price = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("BSR")
            try:
                best_seller_rank = helper.get_best_seller_rank_1(response, self.time)[
                    "value"
                ]
                if best_seller_rank == "NA":
                    best_seller_rank = helper.get_best_seller_rank_2(
                        response, self.time
                    )["value"]
            except Exception:
                best_seller_rank = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("fulfilled")
            try:
                fullfilled = helper.get_fullfilled(response, self.time)["value"]
            except Exception:
                fullfilled = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("rating")
            try:
                rating = helper.get_rating(response, self.time)["value"]
            except Exception:
                rating = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("reviews")
            try:
                total_reviews = helper.get_total_reviews(response, self.time)["value"]
            except Exception:
                total_reviews = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("availability")
            try:
                availability = helper.get_availability(response, self.time)["value"]
            except Exception:
                availability = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("sub discount")
            try:
                subscription_discount = helper.get_subscription_discount(
                    response, self.time
                )["value"]
            except Exception:
                subscription_discount = "NA"
                failed = True

        if not failed:
            if self.debug:
                print("Assigning")

            item["product_name"] = title
            item["product_brand"] = brand
            item["product_offers"] = offers
            item["product_original_price"] = original_price
            item["product_category"] = category
            item["product_icons"] = icons
            item["product_details"] = product_details
            item["product_asin"] = asin
            item["product_important_information"] = important_information
            item["product_description"] = product_description
            item["product_bought_together"] = bought_together
            item["product_variations"] = variations

            item["product_sale_price"] = sale_price
            item["product_rating"] = rating
            item["product_total_reviews"] = total_reviews
            item["product_best_seller_rank"] = best_seller_rank
            item["product_fullfilled"] = fullfilled
            item["product_availability"] = availability
            item["product_subscription_discount"] = subscription_discount

        if failed:
            yield None
        else:
            self.remove_from_failed("data", params)

            ############# TEMP ################
            if self.debug:
                # if sale_price == "NA" or sale_price is "":
                with open("fails/SP_{}.html".format(asin), "w", encoding="utf-8") as f:
                    f.write(response.text)
                # if original_price == "NA" or original_price is "":
                # with open('fails/havells/OP_{}.html'.format(asin), 'w', encoding='utf-8') as f:
                #     f.write(response.text)
                # print("**DEBUG:**/n {}".format(item))
                ####################################
                print("Sending to pipeline")
            yield item
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[2]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[3]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span
            # //*[@id="search"]/div[1]/div[1]/div/span[3]/div[2]/div[4]/div/span/div/div/div/div/div[2]/div[1]/div/span/a/span[2]/span

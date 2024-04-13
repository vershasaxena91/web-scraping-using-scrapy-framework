# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

from urllib.parse import quote
import pymongo
from itemadapter import ItemAdapter
from amazon_product_scraping.items import (
    AmazonSearchProductList,
    AmazonProductInfoItem,
    AmazonProductDailyMovementItem,
    AmazonShareOfSearchItem,
    AmazonProductCommentsItem,
    AmazonShareOfSearchRanksItem,
    AmazonQuestionsItem,
    AmazonTotalQuestionsItem,
)
from amazon_product_scraping.items import (
    FlipkartSearchProductList,
    FlipkartProductInfoItem,
    FlipkartProductDailyMovementItem,
    FlipkartShareOfSearchItem,
    FlipkartRankItem,
    FlipkartProductCommentsItem,
    FlipkartShareOfSearchRanksItem,
)

from amazon_product_scraping.items import (
    ShopeeSearchProductList,
    ShopeeProductInfoItem,
    ShopeeProductDailyMovementItem,
    ShopeeProductCommentsItem,
    ShopeeShareOfSearchRanksItem,
    ShopeeShareOfSearchItem,
)


class CommentsToMongoPipeline:
    """
    A class used to save the scraped item in MongoDB.

    Attributes
    ----------
    collection_name : str
        MongoDB collection name
    mongo_uri : str
        MongoDB address
    mongo_db : str
        MongoDB database name
    """

    input_collection_name = "product_list"
    output_collection_name = "product_comments"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print("*******\nComments Pipeline Started : {}\n********".format(self.mongo_db))

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                # spider.urls.append(prod['product_url'])
                if spider.count > 0:
                    # for i in range(1, 1+math.ceil(spider.count/10)):
                    i = 1
                    spider.urls.append(
                        "https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber={}".format(
                            prod["product_asin"], i
                        )
                    )
                    # spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=five_star&pageNumber={}".format(prod['product_asin'], i))
                    # spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=four_star&pageNumber={}".format(prod['product_asin'], i))
                    # spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=three_star&pageNumber={}".format(prod['product_asin'], i))
                    # spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=two_star&pageNumber={}".format(prod['product_asin'], i))
                    # spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=one_star&pageNumber={}".format(prod['product_asin'], i))
                    # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber={}".format(
                    #     prod['product_asin'], i
                    # ).encode('utf-8'))))
                else:
                    spider.urls.append(
                        "https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber={}".format(
                            prod["product_asin"], 1
                        )
                    )
                    # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber={}".format(
                    #     prod['product_asin'], 1
                    # ).encode('utf-8'))))
        # if spider.cold_run:
        #     asins_list = pd.read_csv('../data_csvs/asins.csv')
        #     for asin in asins_list['ASIN']:
        #         print(asin)
        #         for i in range(1, 1+math.ceil(spider.count/10)):
        #             spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=five_star&pageNumber={}".format(asin, i))
        #             spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=four_star&pageNumber={}".format(asin, i))
        #             spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=three_star&pageNumber={}".format(asin, i))
        #             spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=two_star&pageNumber={}".format(asin, i))
        #             spider.urls.append("https://www.amazon.in/Himalaya-Herbals-Anti-Shampoo-400ml/product-reviews/{}/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&sortBy=recent&filterByStar=one_star&pageNumber={}".format(asin, i))

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "AmazonProductCommentsSpider" and isinstance(
            item, AmazonProductCommentsItem
        ):
            # existing_item = self.db[self.collection_name_comments].find_one(
            #     {"product_asin": item["product_asin"]}
            # )
            # print("****ITEM:\n", item)
            print(len(item["product_comments"]))
            spider.success_counts["prods_checked"] += 1
            spider.success_counts["prods_with_new_comms"] += (
                1 if len(item["product_comments"]) > 0 else 0
            )
            spider.success_counts["new_comments"] += len(item["product_comments"])

            for comment in item["product_comments"]:
                comment["date"] = comment["date"].strftime("%Y-%m-%d %H:%M:%S")
                self.db[self.output_collection_name].find_one_and_update(
                    {"product_asin": item["product_asin"]},
                    {
                        "$push": {
                            "comments": comment,
                        }
                    },
                    upsert=True,
                )
            return item


class NewListingProductURLToMongoPipeline:

    collection_name = "product_list"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "*******\nNew Listing URL Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if (
            spider.name == "AmazonTop100BSRSpider"
            or spider.name == "AmazonSearchListSpider"
            or spider.name == "AmazonBrandSearchListSpider"
        ):
            if isinstance(item, AmazonSearchProductList):
                present = 0
                not_present = 0
                for product in item["products"]:
                    existing_item = self.db[self.collection_name].find_one(
                        {"product_asin": product["product_asin"]}
                    )
                    # print("*****\nITEM:", item)
                    # print(product, "Not Present" if existing_item is None else "Present")
                    if existing_item is None:
                        not_present += 1
                    else:
                        present += 1
                    self.db[self.collection_name].find_one_and_update(
                        {"product_asin": product["product_asin"]},
                        {
                            "$set": {
                                "product_url": product["product_url"],
                            }
                        },
                        upsert=True,
                    )
                    client_added = self.db[self.collection_name].find_one(
                        {
                            "product_asin": product["product_asin"],
                            "clients": self.company_client,
                        }
                    )
                    if not client_added:
                        self.db[self.collection_name].find_one_and_update(
                            {"product_asin": product["product_asin"]},
                            {
                                "$push": {
                                    "clients": self.company_client,
                                }
                            },
                        )
                print("New Added: {}, Hit: {}".format(not_present, present))
                spider.success_counts["new"] += not_present
                spider.success_counts["existing"] += present
        return item


class AmazonProductInfoToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(mongo_uri=crawler.settings.get("MONGO_URI"))

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Info Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            present_prods = set(
                map(
                    lambda x: x["product_asin"],
                    list(
                        self.db[self.output_collection_name].find(
                            {}, {"_id": 0, "product_asin": 1}
                        )
                    ),
                )
            )
            # urls = {}
            # spider.urls = []
            # print(present_prods)
            # print(len(present_prods))
            print("*****\nTo Add:")
            for prod in input_prods:
                if spider.force_info_scrape or (
                    prod["product_asin"] not in present_prods
                ):
                    print(prod["product_asin"])
                    spider.urls.append(prod["product_url"])
                    # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))
            print("END\n*****")

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "AmazonProductInfoSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, AmazonProductInfoItem):
                # existing_item = self.db[self.collection_name].find_one(
                #     {"product_asin": item["product_asin"]}
                # )
                # print(item)
                spider.success_counts["added"] += 1
                self.db[self.output_collection_name].find_one_and_update(
                    {"product_asin": item["product_asin"]},
                    # {"$set": {"product_fullfilled": item["product_fullfilled"]}},
                    {"$set": ItemAdapter(item).asdict()},
                    upsert=True,
                )

        return item


class AmazonProductDailyMovementToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Daily Movement Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(
                self.db[self.input_collection_name].find(
                    {}, {"_id": 0, "product_asin": 0}
                )
            )
            # spider.urls = []
            for prod in input_prods:
                spider.urls.append(prod["product_url"])
                # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "AmazonProductSalePriceBSRSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, AmazonProductDailyMovementItem):
                existing_item = self.db[self.output_collection_name].find_one(
                    {"product_asin": item["product_asin"]}
                )
                if existing_item and item["product_asin"] != "NA":
                    spider.success_counts["added"] += 1
                    # print("****ITEM:\n", item)
                    self.db[self.output_collection_name].find_one_and_update(
                        {"product_asin": item["product_asin"]},
                        # {"$push": {"product_total_questions": item["product_total_questions"]}},
                        {
                            "$push": {
                                "product_sale_price": item["product_sale_price"],
                                "product_best_seller_rank": item[
                                    "product_best_seller_rank"
                                ],
                                "product_fullfilled": item["product_fullfilled"],
                                "product_availability": item["product_availability"],
                                "product_subscription_discount": item[
                                    "product_subscription_discount"
                                ],
                                "product_rating": item["product_rating"],
                                "product_total_reviews": item["product_total_reviews"],
                                "product_original_price": item["product_original_price"]
                                # "product_total_questions": item[
                                #     "product_total_questions"
                                # ],
                            }
                        },
                        upsert=True,
                    )
        return item


class ShareOfSearchPipeline:

    list_collection_name = "share_of_search"
    data_collection_name = "sos_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "********\nShare of Search Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        spider.present_check_func = self.present_check
        if spider.cold_run:
            for keyword in spider.keywords:
                spider.urls.append(
                    "https://www.amazon.in/s?k={}".format(
                        quote(keyword).replace("%20", "+")
                    )
                )

    def present_check(self, filter):
        print(filter)
        print(self.db[self.data_collection_name].find_one(filter) is not None)
        return self.db[self.data_collection_name].find_one(filter) is not None

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "AmazonShareOfSearchSpider":
            if isinstance(item, AmazonShareOfSearchRanksItem):
                for prod in item["product_ranks"]:
                    spider.success_counts["ranked"] += 1
                    self.db[self.list_collection_name].find_one_and_update(
                        {
                            "time": spider.time,
                            "keyword": prod["keyword"],
                            "clients": self.company_client,
                        },
                        {
                            "$push": {
                                "product_order": {
                                    "product_asin": prod["product_asin"],
                                    "product_rank": prod["product_rank"],
                                    "sponsored": prod["sponsored"],
                                }
                            }
                        },
                        upsert=True,
                    )

            elif isinstance(item, AmazonShareOfSearchItem):
                spider.success_counts["added"] += 1
                print(str(ItemAdapter(item).asdict()).encode("utf-8"))
                self.db[self.data_collection_name].find_one_and_update(
                    {"product_asin": item["product_asin"]},
                    {
                        "$set": ItemAdapter(item).asdict(),
                    },
                    upsert=True,
                )
        return item


class QuestionsToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_QAs"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nAmazon Question-Answers Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                # spider.urls.append(prod['product_url'])
                # https://www.amazon.in/ask/questions/asin/{asin}/{page}?sort={HELPFUL/SUBMIT_DATE}&isAnswered={boolean}
                spider.urls.append(
                    "https://www.amazon.in/ask/questions/asin/{}/{}?sort=SUBMIT_DATE&isAnswered=true".format(
                        prod["product_asin"], 1
                    )
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "AmazonProductQuestionsSpider" and isinstance(
            item, AmazonQuestionsItem
        ):
            print(str(item).encode("utf-8"))
            print(len(item["product_QAs"]))
            spider.success_counts["prods_checked"] += 1
            spider.success_counts["prods_with_new_QAs"] += (
                1 if len(item["product_QAs"]) > 0 else 0
            )
            spider.success_counts["new_QAs"] += len(item["product_QAs"])

            for qa in item["product_QAs"]:
                qa["date"] = qa["date"].strftime("%Y-%m-%d %H:%M:%S")
                self.db[self.output_collection_name].find_one_and_update(
                    {"product_asin": item["product_asin"]},
                    {
                        "$push": {
                            "QAs": qa,
                        }
                    },
                    upsert=True,
                )
            return item


class AmazonProductTotalQuestionsToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nAmazon Total Questions Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                # spider.urls.append(prod['product_url'])
                # https://www.amazon.in/ask/questions/asin/{asin}/{page}?sort={HELPFUL/SUBMIT_DATE}&isAnswered={boolean}
                spider.urls.append(
                    "https://www.amazon.in/ask/questions/asin/{}/{}?sort=SUBMIT_DATE&isAnswered=true".format(
                        prod["product_asin"], 1
                    )
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "AmazonProductTotalQuestionsSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, AmazonTotalQuestionsItem):
                existing_item = self.db[self.output_collection_name].find_one(
                    {"product_asin": item["product_asin"]}
                )
                if existing_item and item["product_asin"] != "NA":
                    spider.success_counts["added"] += 1
                    # print("****ITEM:\n", item)
                    self.db[self.output_collection_name].find_one_and_update(
                        {"product_asin": item["product_asin"]},
                        {
                            "$push": {
                                "product_total_questions": item[
                                    "product_total_questions"
                                ]
                            }
                        },
                        upsert=True,
                    )
        return item


class FlipkartNewListingProductURLToMongoPipeline:

    collection_name = "product_list"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "*******\nNew Listing URL Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "FlipkartSearchListSpider":
            if isinstance(item, FlipkartSearchProductList):
                present = 0
                not_present = 0
                for product in item["products"]:
                    existing_item = self.db[self.collection_name].find_one(
                        {
                            "product_pid": product["product_pid"],
                            # "product_lid": product["product_lid"],
                            "marketplace": product["marketplace"],
                        },
                    )
                    # print("*****\nITEM:", item)
                    # print(product, "Not Present" if existing_item is None else "Present")
                    if existing_item is None:
                        not_present += 1
                    else:
                        present += 1
                    self.db[self.collection_name].find_one_and_update(
                        {
                            "product_pid": product["product_pid"],
                            "marketplace": product["marketplace"],
                        },
                        {
                            "$set": {
                                "product_lid": product["product_lid"],
                                "product_url": product["product_url"],
                            }
                        },
                        upsert=True,
                    )
                    client_added = self.db[self.collection_name].find_one(
                        {
                            "product_pid": product["product_pid"],
                            "marketplace": product["marketplace"],
                            "clients": self.company_client,
                        }
                    )
                    if not client_added:
                        self.db[self.collection_name].find_one_and_update(
                            {
                                "product_pid": product["product_pid"],
                                "marketplace": product["marketplace"],
                            },
                            {
                                "$push": {
                                    "clients": self.company_client,
                                }
                            },
                        )
                print("New Added: {}, Hit: {}".format(not_present, present))
                spider.success_counts["new"] += not_present
                spider.success_counts["existing"] += present
        return item


class FlipkartProductInfoToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(mongo_uri=crawler.settings.get("MONGO_URI"))

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        def hash(pid, marketplace):
            return pid + "--" + marketplace

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Info Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            present_prods = set()
            for p in list(
                self.db[self.output_collection_name].find(
                    {}, {"_id": 0, "product_pid": 1, "marketplace": 1, "scraped_on": 1}
                )
            ):
                if "scraped_on" in p.keys():
                    present_prods.add(hash(p["product_pid"], p["marketplace"]))
            # print(presents)
            # present_prods = set(map(lambda x: hash(x['product_pid'], x['marketplace']), list(self.db[self.output_collection_name].find({}, {"_id": 0, "product_pid": 1, "product_lid": 1, "marketplace": 1, "scraped_on": 1}))))
            # urls = {}
            # spider.urls = []
            # print(present_prods)
            print(len(present_prods))
            print("*****\nTo Add:")
            i = 0
            for prod in input_prods:
                if spider.force_info_scrape or (
                    hash(prod["product_pid"], prod["marketplace"]) not in present_prods
                ):
                    i += 1
                    print(i, prod["product_pid"])
                    # spider.urls.append(prod['product_url'])
                    # spider.urls.append('{}?pid={}&marketplace={}'.format(prod['product_url'].split('?')[0], prod['product_pid'], prod['marketplace']))
                    spider.urls.append(
                        {
                            "url": "{}?pid={}&marketplace".format(
                                prod["product_url"].split("?")[0],
                                prod["product_pid"],
                                "FLIPKART",
                            ),
                            "marketplace": prod["marketplace"],
                        }
                    )
                    print(spider.urls[-1])
                    # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))
            print("END\n*****")

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "FlipkartProductInfoSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, FlipkartProductInfoItem):
                # existing_item = self.db[self.collection_name].find_one(
                #     {"product_asin": item["product_asin"]}
                # )
                # print(item)
                spider.success_counts["added"] += 1
                # self.db[self.output_collection_name].insert_one(ItemAdapter(item).asdict())
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_pid": item["product_pid"],
                        # "product_lid": item["product_lid"],
                        "marketplace": item["marketplace"],
                    },
                    {"$set": ItemAdapter(item).asdict()},
                    upsert=True,
                )

        return item


class FlipkartProductDailyMovementToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Daily Movement Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            spider.urls = []
            for prod in input_prods:
                # spider.urls.append(prod['product_url'])
                # spider.urls.append('{}?pid={}&marketplace={}'.format(prod['product_url'].split('?')[0], prod['product_pid'], prod['marketplace']))
                spider.urls.append(
                    {
                        "url": "{}?pid={}&marketplace".format(
                            prod["product_url"].split("?")[0],
                            prod["product_pid"],
                            "FLIPKART",
                        ),
                        "marketplace": prod["marketplace"],
                    }
                )

                # print(spider.urls[-1])
                # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "FlipkartProductSalePriceSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, FlipkartProductDailyMovementItem):
                existing_item = self.db[self.output_collection_name].find_one(
                    {
                        "product_pid": item["product_pid"],
                        # "product_lid": item["product_lid"],
                        "marketplace": item["marketplace"],
                    },
                )
                if existing_item and item["product_pid"] != "NA":
                    spider.success_counts["added"] += 1
                    # print("****ITEM:\n", item)
                    self.db[self.output_collection_name].find_one_and_update(
                        {
                            "product_pid": item["product_pid"],
                            # "product_lid": item["product_lid"],
                            "marketplace": item["marketplace"],
                        },
                        {
                            "$push": {
                                "product_sale_price": item["product_sale_price"],
                                "product_availability": item["product_availability"],
                                "product_rating": item["product_rating"],
                                "product_total_reviews": item["product_total_reviews"],
                                "product_total_ratings": item["product_total_ratings"],
                                "product_assured": item["product_assured"],
                            },
                        },
                        upsert=True,
                    )
                    if (
                        isinstance(item["product_original_price"], dict)
                        and "value" in item["product_original_price"].keys()
                        and item["product_original_price"]["value"]
                    ):
                        self.db[self.output_collection_name].find_one_and_update(
                            {
                                "product_pid": item["product_pid"],
                                # "product_lid": item["product_lid"],
                                "marketplace": item["marketplace"],
                            },
                            {
                                "$set": {
                                    "product_original_price": item[
                                        "product_original_price"
                                    ]["value"]
                                }
                            },
                        )
        return item


class FlipkartShareOfSearchPipeline:

    list_collection_name = "share_of_search"
    data_collection_name = "sos_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "********\nShare of Search Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        spider.present_check_func = self.present_check
        if spider.cold_run:
            for keyword in spider.keywords:
                spider.urls.append(
                    "https://www.flipkart.com/search?q={}".format(quote(keyword))
                )

    def present_check(self, filter):
        # print(filter)
        # print(self.db[self.data_collection_name].find_one(filter) is not None)
        return self.db[self.data_collection_name].find_one(filter) is not None

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "FlipkartShareOfSearchSpider":
            if isinstance(item, FlipkartShareOfSearchRanksItem):
                for prod in item["product_ranks"]:
                    spider.success_counts["ranked"] += 1
                    self.db[self.list_collection_name].find_one_and_update(
                        {
                            "time": spider.time,
                            "keyword": prod["keyword"],
                            "clients": self.company_client,
                        },
                        {
                            "$push": {
                                "product_order": {
                                    "product_pid": prod["product_pid"],
                                    "marketplace": prod["marketplace"],
                                    "product_rank": prod["product_rank"],
                                }
                            }
                        },
                        upsert=True,
                    )
            elif isinstance(item, FlipkartShareOfSearchItem):
                spider.success_counts["added"] += 1
                self.db[self.data_collection_name].find_one_and_update(
                    {
                        "product_pid": item["product_pid"],
                        "marketplace": item["marketplace"],
                    },
                    {"$set": ItemAdapter(item).asdict()},
                    upsert=True,
                )
        return item


class FlipkartProductRankPipeline:

    list_collection_name = "product_list"
    data_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "*******\nFlipkart Product Rank Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "FlipkartProductRankSpider":
            if isinstance(item, FlipkartRankItem):
                found = 0
                present = 0
                not_present = 0
                for product in item["product_ranks"]:
                    if product["product_pid"] != "NA":
                        found += 1
                        if product["add"] == "push":
                            self.db[self.data_collection_name].find_one_and_update(
                                {
                                    "product_pid": product["product_pid"],
                                    # "product_lid": product["product_lid"],
                                    "marketplace": product["marketplace"],
                                },
                                {
                                    "$push": {
                                        "product_rank": product["product_rank"],
                                    }
                                },
                                upsert=spider.upsert,
                            )
                        elif product["add"] == "reset":
                            self.db[self.data_collection_name].find_one_and_update(
                                {
                                    "product_pid": product["product_pid"],
                                    # "product_lid": product["product_lid"],
                                    "marketplace": product["marketplace"],
                                    "product_rank.time": product["product_rank"][
                                        "time"
                                    ],
                                },
                                {
                                    "$set": {
                                        "product_rank.$.value": product["product_rank"][
                                            "value"
                                        ],
                                    }
                                },
                                upsert=spider.upsert,
                            )
                        existing_item = self.db[self.list_collection_name].find_one(
                            {
                                "product_pid": product["product_pid"],
                                # "product_lid": product["product_lid"],
                                "marketplace": product["marketplace"],
                            }
                        )
                        if existing_item is None:
                            not_present += 1
                            self.db[self.list_collection_name].find_one_and_update(
                                {
                                    "product_pid": product["product_pid"],
                                    # "product_lid": product["product_lid"],
                                    "marketplace": product["marketplace"],
                                },
                                {
                                    "$set": {
                                        "product_lid": product["product_lid"],
                                        "product_url": product["product_url"],
                                    }
                                },
                                upsert=spider.upsert,
                            )
                        else:
                            present += 1
                        client_added = self.db[self.list_collection_name].find_one(
                            {
                                "product_pid": product["product_pid"],
                                "marketplace": product["marketplace"],
                                "clients": self.company_client,
                            }
                        )
                        if not client_added:
                            self.db[self.list_collection_name].find_one_and_update(
                                {
                                    "product_pid": product["product_pid"],
                                    "marketplace": product["marketplace"],
                                },
                                {
                                    "$push": {
                                        "clients": self.company_client,
                                    }
                                },
                            )
                spider.success_counts["found"] += found
                spider.success_counts["new"] += not_present
                spider.success_counts["added"] += (
                    (present + not_present) if spider.upsert else present
                )

        return item


# https://www.flipkart.com/head-shoulders-anti-hair-fall-shampoo-360ml/p/itmffvtwmtfmrpby?pid=SMPFFVGSCZ9JMU8B&lid=LSTSMPFFVGSCZ9JMU8B92Q2F4&marketplace=FLIPKART&q=shampoo&store=g9b%2Flcf%2Fqqm%2Ft36&srno=s_3_100&otracker=search&otracker1=search&fm=organic&iid=2d720238-1e7e-474a-a637-aa1d85a5c8ef.SMPFFVGSCZ9JMU8B.SEARCH&ppt=None&ppn=None&ssid=718tat5h9s0000001641302776577&qH=186764a607df448c
# https://www.flipkart.com/head-shoulders-anti-hair-fall-shampoo-360ml/p/itmffvtwmtfmrpby?pid=SMPFFVGSCZ9JMU8B&lid=LSTSMPFFVGSCZ9JMU8B92Q2F4&marketplace=FLIPKART&q=shampoo&store=g9b%2Flcf%2Fqqm%2Ft36&srno=s_3_110&otracker=search&otracker1=search&fm=organic&iid=21a1d23d-a8bd-4b07-8b43-a5b50865a325.SMPFFVGSCZ9JMU8B.SEARCH&ppt=None&ppn=None&ssid=5ibti7unr40000001641364501870&qH=186764a607df448c


class FlipkartCommentsToMongoPipeline:
    """
    A class used to save the scraped item in MongoDB.

    Attributes
    ----------
    collection_name : str
        MongoDB collection name
    mongo_uri : str
        MongoDB address
    mongo_db : str
        MongoDB database name
    """

    input_collection_name = "product_list"
    output_collection_name = "product_comments"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print("*******\nComments Pipeline Started : {}\n********".format(self.mongo_db))

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                # spider.urls.append(prod['product_url'])
                if spider.count > 0:
                    # for i in range(1, 1+math.ceil(spider.count/10)):
                    i = 1
                    spider.urls.append(
                        {
                            "url": "https://www.flipkart.com/head-shoulders-smooth-silky-anti-dandruff-shampoo/product-reviews/itmc8a771e408aac?pid={}&marketplace={}&sortOrder=MOST_RECENT&page={}".format(
                                prod["product_pid"], "FLIPKART", i
                            ),
                            "marketplace": prod["marketplace"],
                        }
                    )
                else:
                    spider.urls.append(
                        {
                            "url": "https://www.flipkart.com/head-shoulders-smooth-silky-anti-dandruff-shampoo/product-reviews/itmc8a771e408aac?pid={}&marketplace={}&sortOrder=MOST_RECENT&page={}".format(
                                prod["product_pid"], "FLIPKART", 1
                            ),
                            "marketplace": prod["marketplace"],
                        }
                    )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "FlipkartProductCommentsSpider" and isinstance(
            item, FlipkartProductCommentsItem
        ):
            # existing_item = self.db[self.collection_name_comments].find_one(
            #     {"product_asin": item["product_asin"]}
            # )
            # print("****ITEM:\n", item)
            print(len(item["product_comments"]))
            spider.success_counts["prods_checked"] += 1
            spider.success_counts["prods_with_new_comms"] += (
                1 if len(item["product_comments"]) > 0 else 0
            )
            spider.success_counts["new_comments"] += len(item["product_comments"])

            for comment in item["product_comments"]:
                comment["date"] = comment["date"].strftime("%Y-%m-%d %H:%M:%S")
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_asin": item["product_pid"],
                        "marketplace": item["marketplace"],
                    },
                    {
                        "$set": {"scraped_on": item["scraped_on"]},
                        "$push": {
                            "comments": comment,
                        },
                    },
                    upsert=True,
                )
            return item


class ShopeeNewListingProductURLToMongoPipeline:

    collection_name = "product_list"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "*******\nNew Listing URL Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

        if spider.cold_run:
            for brand_id in spider.brand_ids:
                spider.urls.append(
                    "https://shopee.co.id/api/v4/search/search_items?by=pop&limit=100&order=desc&page_type=shop&scenario=PAGE_OTHERS&version=2&match_id={}&shop_categoryids={}".format(
                        brand_id[0], brand_id[1]
                    )
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "ShopeeBrandSearchListSpider":
            if isinstance(item, ShopeeSearchProductList):
                present = 0
                not_present = 0
                for product in item["products"]:
                    existing_item = self.db[self.collection_name].find_one(
                        {
                            "product_itemid": product["product_itemid"],
                            "product_shopid": product["product_shopid"],
                        },
                    )
                    # print("*****\nITEM:", item)
                    # print(product, "Not Present" if existing_item is None else "Present")
                    if existing_item is None:
                        not_present += 1
                    else:
                        present += 1
                    self.db[self.collection_name].find_one_and_update(
                        {
                            "product_itemid": product["product_itemid"],
                            "product_shopid": product["product_shopid"],
                        },
                        {
                            "$set": {
                                "product_url": product["product_url"],
                            }
                        },
                        upsert=True,
                    )
                    client_added = self.db[self.collection_name].find_one(
                        {
                            "product_itemid": product["product_itemid"],
                            "product_shopid": product["product_shopid"],
                            "clients": self.company_client,
                        }
                    )
                    if not client_added:
                        self.db[self.collection_name].find_one_and_update(
                            {
                                "product_itemid": product["product_itemid"],
                                "product_shopid": product["product_shopid"],
                            },
                            {
                                "$push": {
                                    "clients": self.company_client,
                                }
                            },
                        )
                print("New Added: {}, Hit: {}".format(not_present, present))
                spider.success_counts["new"] += not_present
                spider.success_counts["existing"] += present
        return item


class ShopeeProductInfoToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(mongo_uri=crawler.settings.get("MONGO_URI"))

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        def hash(itemid, shopid):
            return itemid + "--" + shopid

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Info Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            present_prods = set()
            for p in list(
                self.db[self.output_collection_name].find(
                    {},
                    {
                        "_id": 0,
                        "product_itemid": 1,
                        "product_shopid": 1,
                        "scraped_on": 1,
                    },
                )
            ):
                if "scraped_on" in p.keys():
                    present_prods.add(hash(p["product_itemid"], p["product_shopid"]))
            # print(presents)
            # present_prods = set(map(lambda x: hash(x['product_pid'], x['marketplace']), list(self.db[self.output_collection_name].find({}, {"_id": 0, "product_pid": 1, "product_lid": 1, "marketplace": 1, "scraped_on": 1}))))
            # urls = {}
            # spider.urls = []
            # print(present_prods)
            print(len(present_prods))
            print("*****\nTo Add:")
            i = 0
            for prod in input_prods:
                if spider.force_info_scrape or (
                    hash(prod["product_itemid"], prod["product_shopid"])
                    not in present_prods
                ):
                    i += 1
                    print(i, prod["product_itemid"])
                    # spider.urls.append(prod['product_url'])
                    # spider.urls.append('{}?pid={}&marketplace={}'.format(prod['product_url'].split('?')[0], prod['product_pid'], prod['marketplace']))
                    spider.urls.append(
                        {
                            "url": "{}?itemid={}&shopid={}".format(
                                prod["product_url"].split("?")[0],
                                prod["product_itemid"],
                                prod["product_shopid"],
                            ),
                        }
                    )
                    print(spider.urls[-1])
                    # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))
            print("END\n*****")

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "ShopeeProductInfoSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, ShopeeProductInfoItem):
                spider.success_counts["added"] += 1
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_itemid": item["product_itemid"],
                        "product_shopid": item["product_shopid"],
                    },
                    {"$set": ItemAdapter(item).asdict()},
                    upsert=True,
                )

        return item


class ShopeeProductDailyMovementToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Daily Movement Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            spider.urls = []
            for prod in input_prods:
                # spider.urls.append(prod['product_url'])
                # spider.urls.append('{}?pid={}&marketplace={}'.format(prod['product_url'].split('?')[0], prod['product_pid'], prod['marketplace']))
                spider.urls.append(
                    {
                        "url": "{}?itemid={}&shopid={}".format(
                            prod["product_url"].split("?")[0],
                            prod["product_itemid"],
                            prod["product_shopid"],
                        ),
                    }
                )

                # print(spider.urls[-1])
                # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "ShopeeProductDailyMoveSpider":
            spider.success_counts["new"] += 1
            if isinstance(item, ShopeeProductDailyMovementItem):
                existing_item = self.db[self.output_collection_name].find_one(
                    {
                        "product_itemid": item["product_itemid"],
                        "product_shopid": item["product_shopid"],
                    },
                )
                if existing_item and item["product_itemid"] != "":
                    spider.success_counts["added"] += 1
                    # print("****ITEM:\n", item)
                    self.db[self.output_collection_name].find_one_and_update(
                        {
                            "product_itemid": item["product_itemid"],
                            "product_shopid": item["product_shopid"],
                        },
                        {
                            "$push": {
                                "product_sale_price": item["product_sale_price"],
                                "product_stock": item["product_stock"],
                                "product_rating": item["product_rating"],
                                "product_total_likes": item["product_total_likes"],
                                "product_total_ratings": item["product_total_ratings"],
                                "product_sold": item["product_sold"],
                                "product_historical_sold": item[
                                    "product_historical_sold"
                                ],
                                "product_discount": item["product_discount"],
                            },
                        },
                        upsert=True,
                    )
                    if (
                        isinstance(item["product_original_price"], dict)
                        and "value" in item["product_original_price"].keys()
                        and item["product_original_price"]["value"]
                    ):
                        self.db[self.output_collection_name].find_one_and_update(
                            {
                                "product_itemid": item["product_itemid"],
                                "product_shopid": item["product_shopid"],
                            },
                            {
                                "$set": {
                                    "product_original_price": item[
                                        "product_original_price"
                                    ]["value"]
                                }
                            },
                        )
        return item


class ShopeeCommentsToMongoPipeline:
    """
    A class used to save the scraped item in MongoDB.

    Attributes
    ----------
    collection_name : str
        MongoDB collection name
    mongo_uri : str
        MongoDB address
    mongo_db : str
        MongoDB database name
    """

    input_collection_name = "product_list"
    output_collection_name = "product_comments"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        print("*******\nComments Pipeline Started : {}\n********".format(self.mongo_db))

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                spider.urls.append(
                    {
                        # "url": "https://shopee.co.id/api/v2/item/get_ratings?itemid={}&shopid={}&type=0&limit={}&offset={}".format(
                        #     prod["product_itemid"], prod["product_shopid"], 50, 0
                        "url": "https://shopee.co.id/api/v2/item/get_ratings?filter=0&flag=1&itemid={}&limit=6&offset=0&shopid={}&type=0".format(
                            prod["product_itemid"], prod["product_shopid"]
                        ),
                    }
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "ShopeeProductCommentsSpider" and isinstance(
            item, ShopeeProductCommentsItem
        ):
            # existing_item = self.db[self.collection_name_comments].find_one(
            #     {"product_asin": item["product_asin"]}
            # )
            # print("****ITEM:\n", item)
            print(len(item["product_comments"]))
            spider.success_counts["prods_checked"] += 1
            spider.success_counts["prods_with_new_comms"] += (
                1 if len(item["product_comments"]) > 0 else 0
            )
            spider.success_counts["new_comments"] += len(item["product_comments"])

            for comment in item["product_comments"]:
                comment["date"] = comment["date"].strftime("%Y-%m-%d %H:%M:%S")
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_itemid": item["product_itemid"],
                        "product_shopid": item["product_shopid"],
                    },
                    {
                        "$set": {"scraped_on": item["scraped_on"]},
                        "$push": {
                            "comments": comment,
                        },
                    },
                    upsert=True,
                )
            return item


class ShopeeShareOfSearchPipeline:

    list_collection_name = "share_of_search"
    data_collection_name = "sos_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "********\nShare of Search Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        spider.present_check_func = self.present_check
        if spider.cold_run:
            for keyword in spider.keywords:
                spider.urls.append(
                    "https://shopee.co.id/api/v4/search/search_items?by=relevancy&keyword={}&limit=100&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2".format(
                        quote(keyword)
                    )
                )

    def present_check(self, filter):
        # print(filter)
        # print(self.db[self.data_collection_name].find_one(filter) is not None)
        return self.db[self.data_collection_name].find_one(filter) is not None

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "ShopeeShareOfSearchSpider":
            if isinstance(item, ShopeeShareOfSearchRanksItem):
                for prod in item["product_ranks"]:
                    spider.success_counts["ranked"] += 1
                    self.db[self.list_collection_name].find_one_and_update(
                        {
                            "time": spider.time,
                            "keyword": prod["keyword"],
                            "clients": self.company_client,
                        },
                        {
                            "$push": {
                                "product_order": {
                                    "product_itemid": prod["product_itemid"],
                                    "product_shopid": prod["product_shopid"],
                                    "product_rank": prod["product_rank"],
                                }
                            }
                        },
                        upsert=True,
                    )
            elif isinstance(item, ShopeeShareOfSearchItem):
                spider.success_counts["added"] += 1
                self.db[self.data_collection_name].find_one_and_update(
                    {
                        "product_itemid": item["product_itemid"],
                        "product_shopid": item["product_shopid"],
                    },
                    {"$set": ItemAdapter(item).asdict()},
                    upsert=True,
                )
        return item


class NykaaNewListingProductURLToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "*******\nNew Listing URL Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

        if spider.cold_run:
            for brand_id in spider.brand_ids:
                spider.urls.append(
                    "https://www.nykaa.com/app-api/index.php/products/list?brand_filter={}&category_id={}&client=react&filter_format=v2&page_no=1".format(
                        brand_id[0], brand_id[1]
                    )
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """
        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "NykaaBrandSearchListSpider":
            present = 0
            not_present = 0
            for product in item["products"]:
                existing_item = self.db[self.input_collection_name].find_one(
                    {
                        "product_id": product["product_id"],
                    },
                )
                # print("*****\nITEM:", item)
                # print(product, "Not Present" if existing_item is None else "Present")
                if existing_item is None:
                    not_present += 1
                else:
                    present += 1
                self.db[self.input_collection_name].find_one_and_update(
                    {
                        "product_id": product["product_id"],
                    },
                    {
                        "$set": {
                            "product_url": product["product_url"],
                        }
                    },
                    upsert=True,
                )
                client_added = self.db[self.input_collection_name].find_one(
                    {
                        "product_id": product["product_id"],
                        "clients": self.company_client,
                    }
                )
                if not client_added:
                    self.db[self.input_collection_name].find_one_and_update(
                        {
                            "product_id": product["product_id"],
                        },
                        {
                            "$push": {
                                "clients": self.company_client,
                            }
                        },
                    )
                self.db[self.output_collection_name].find_one_and_update(
                    {"product_id": product["product_id"]},
                    {
                        "$set": {
                            "product_name": product["product_name"],
                            "product_brand": product["product_brand"],
                            "product_category": product["product_category"],
                        }
                    },
                    upsert=True,
                )
            print("New Added: {}, Hit: {}".format(not_present, present))
            spider.success_counts["new"] += not_present
            spider.success_counts["existing"] += present
        return item


class NykaaProductInfoToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(mongo_uri=crawler.settings.get("MONGO_URI"))

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Info Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            present_prods = set(
                map(
                    lambda x: x["product_id"],
                    list(
                        self.db[self.output_collection_name].find(
                            {}, {"_id": 0, "product_id": 1}
                        )
                    ),
                )
            )
            # urls = {}
            # spider.urls = []
            # print(present_prods)
            # print(len(present_prods))
            print("*****\nTo Add:")
            for prod in input_prods:
                if spider.force_info_scrape or (
                    prod["product_id"] not in present_prods
                ):
                    print(prod["product_id"])
                    spider.urls.append(prod["product_url"])
                    # spider.urls.append("http://api.proxiesapi.com/?auth_key={}&url={}".format("b433886e7d6c73d3c24eeb0d9244f5c6_sr98766_ooPq87", quote(prod['product_url'].encode('utf-8'))))
            print("END\n*****")

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "NykaaProductInfoSpider":
            spider.success_counts["new"] += 1
            # if isinstance(item, NykaaProductInfoItem):
            # existing_item = self.db[self.collection_name].find_one(
            #     {"product_asin": item["product_asin"]}
            # )
            # print(item)
            spider.success_counts["added"] += 1
            self.db[self.output_collection_name].find_one_and_update(
                {"product_id": item["product_id"]},
                # {"$set": {"product_fullfilled": item["product_fullfilled"]}},
                {"$set": ItemAdapter(item).asdict()},
                upsert=True,
            )

        return item


class NykaaProductDailyMovementToMongoPipeline:

    input_collection_name = "product_list"
    output_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db

        print(
            "*******\nProduct Daily Movement Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            spider.urls = []
            for prod in input_prods:
                spider.urls.append({"url": prod["product_url"]})

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "NykaaProductSalePriceSpider":
            spider.success_counts["new"] += 1
            existing_item = self.db[self.output_collection_name].find_one(
                {
                    "product_id": item["product_id"],
                },
            )
            if existing_item and item["product_id"] != "NA":
                spider.success_counts["added"] += 1
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_id": item["product_id"],
                    },
                    {
                        "$push": {
                            "product_sale_price": item["product_sale_price"],
                            "product_original_price": item["product_original_price"],
                            "product_availability": item["product_availability"],
                            "product_rating": item["product_rating"],
                            "product_total_reviews": item["product_total_reviews"],
                            "product_total_ratings": item["product_total_ratings"],
                            "product_discount": item["product_discount"],
                        },
                    },
                    upsert=True,
                )

        return item


class NykaaProductRankPipeline:

    list_collection_name = "product_list"
    data_collection_name = "product_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "*******\nNykaa Product Rank Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "NykaaProductRankSpider":
            found = 0
            present = 0
            not_present = 0
            for product in item["product_ranks"]:
                if product["product_id"] != "NA":
                    found += 1
                    existing_item = self.db[self.list_collection_name].find_one(
                        {
                            "product_id": product["product_id"],
                        }
                    )
                    if existing_item is None:
                        not_present += 1
                    else:
                        present += 1
                        if product["add"] == "push":
                            self.db[self.data_collection_name].find_one_and_update(
                                {
                                    "product_id": product["product_id"],
                                },
                                {
                                    "$push": {
                                        "product_rank": product["product_rank"],
                                    }
                                },
                                upsert=True,
                            )
                        elif product["add"] == "reset":
                            self.db[self.data_collection_name].find_one_and_update(
                                {
                                    "product_id": product["product_id"],
                                    "product_rank.time": product["product_rank"][
                                        "time"
                                    ],
                                },
                                {
                                    "$set": {
                                        "product_rank.$.value": product["product_rank"][
                                            "value"
                                        ],
                                    }
                                },
                                upsert=True,
                            )
                    client_added = self.db[self.list_collection_name].find_one(
                        {
                            "product_id": product["product_id"],
                            "clients": self.company_client,
                        }
                    )
                    if not client_added:
                        self.db[self.list_collection_name].find_one_and_update(
                            {
                                "product_id": product["product_id"],
                            },
                            {
                                "$push": {
                                    "clients": self.company_client,
                                }
                            },
                        )
            spider.success_counts["found"] += found
            spider.success_counts["new"] += not_present
            spider.success_counts["added"] += (
                (present + not_present) if True else present
            )

        return item


class NykaaCommentsToMongoPipeline:
    """
    A class used to save the scraped item in MongoDB.

    Attributes
    ----------
    collection_name : str
        MongoDB collection name
    mongo_uri : str
        MongoDB address
    mongo_db : str
        MongoDB database name
    """

    input_collection_name = "product_list"
    output_collection_name = "product_comments"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        print("*******\nComments Pipeline Started : {}\n********".format(self.mongo_db))

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                spider.urls.append(
                    {
                        # "url": "https://shopee.co.id/api/v2/item/get_ratings?itemid={}&shopid={}&type=0&limit={}&offset={}".format(
                        #     prod["product_itemid"], prod["product_shopid"], 50, 0
                        "url": "https://www.nykaa.com/gateway-api/products/{}/reviews?pageNo=1&sort=MOST_RECENT&domain=nykaa".format(
                            prod["product_id"]
                        ),
                    }
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "NykaaProductCommentsSpider":
            # existing_item = self.db[self.collection_name_comments].find_one(
            #     {"product_asin": item["product_asin"]}
            # )
            # print("****ITEM:\n", item)
            print(len(item["product_comments"]))
            spider.success_counts["prods_checked"] += 1
            spider.success_counts["prods_with_new_comms"] += (
                1 if len(item["product_comments"]) > 0 else 0
            )
            spider.success_counts["new_comments"] += len(item["product_comments"])

            for comment in item["product_comments"]:
                # comment["date"] = comment["date"].strftime("%Y-%m-%d %H:%M:%S")
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_id": item["product_id"],
                    },
                    {
                        "$set": {"scraped_on": item["scraped_on"]},
                        "$push": {
                            "comments": comment,
                        },
                    },
                    upsert=True,
                )
            return item


class NykaaQuestionsToMongoPipeline:
    """
    A class used to save the scraped item in MongoDB.

    Attributes
    ----------
    collection_name : str
        MongoDB collection name
    mongo_uri : str
        MongoDB address
    mongo_db : str
        MongoDB database name
    """

    input_collection_name = "product_list"
    output_collection_name = "product_QAs"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        print(
            "*******\nQuestions Pipeline Started : {}\n********".format(self.mongo_db)
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if spider.cold_run:
            input_prods = list(self.db[self.input_collection_name].find({}, {"_id": 0}))
            # spider.urls = []
            for prod in input_prods:
                spider.urls.append(
                    {
                        # "url": "https://shopee.co.id/api/v2/item/get_ratings?itemid={}&shopid={}&type=0&limit={}&offset={}".format(
                        #     prod["product_itemid"], prod["product_shopid"], 50, 0
                        "url": "https://www.nykaa.com/custom/index/getqna?isAjax=1&p=1&pid={}&sort=newest".format(
                            prod["product_id"]
                        ),
                    }
                )

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """
        if spider.name == "NykaaProductQuestionsSpider":
            # existing_item = self.db[self.collection_name_comments].find_one(
            #     {"product_asin": item["product_asin"]}
            # )
            # print("****ITEM:\n", item)
            print(len(item["product_questions"]))
            spider.success_counts["prods_checked"] += 1
            spider.success_counts["prods_with_new_ques"] += (
                1 if len(item["product_questions"]) > 0 else 0
            )
            spider.success_counts["new_questions"] += len(item["product_questions"])

            for question in item["product_questions"]:
                # comment["date"] = comment["date"].strftime("%Y-%m-%d %H:%M:%S")
                self.db[self.output_collection_name].find_one_and_update(
                    {
                        "product_id": item["product_id"],
                    },
                    {
                        "$set": {"scraped_on": item["scraped_on"]},
                        "$push": {
                            "QAs": question,
                        },
                    },
                    upsert=True,
                )
            return item


class NykaaShareOfSearchPipeline:

    list_collection_name = "share_of_search"
    data_collection_name = "sos_data"

    def __init__(self, mongo_uri):
        self.mongo_uri = mongo_uri

    @classmethod
    def from_crawler(cls, crawler):
        """
        A class method used to pull in information from settings.py.
        """

        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
        )

    def open_spider(self, spider):
        """
        This class method is called when initializing spider and opening MongoDB connection.

        Parameters
        ----------
        spider : object
            the spider which was opened
        """

        self.mongo_db = spider.mongo_db
        self.company_client = spider.company_client

        print(
            "********\nShare of Search Pipeline Started : {}\n********".format(
                self.mongo_db
            )
        )

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        spider.present_check_func = self.present_check
        if spider.cold_run:
            for keyword in spider.keywords:
                spider.urls.append(
                    "https://www.nykaa.com/nyk/aggregator-gludo/api/search.list?app_version=7003&customer_group_id=1&filter_format=v2&from=0&platform=website&search={}&source=react".format(
                        quote(keyword)
                    )
                )

    def present_check(self, filter):
        return self.db[self.data_collection_name].find_one(filter) is not None

    def close_spider(self, spider):
        """
        This class method is called when the spider is closed.

        Parameters
        ----------
        spider : object
            the spider which was closed
        """

        self.client.close()

    def process_item(self, item, spider):
        """
        This class method is called for every item pipeline component.

        Parameters
        ----------
        item : item object
            the scraped item
        spider : object
            the spider which scraped the item
        """

        if spider.name == "NykaaShareOfSearchSpider":
            for prod in item["product_details"]:
                spider.success_counts["ranked"] += 1
                self.db[self.list_collection_name].find_one_and_update(
                    {
                        "time": spider.time,
                        "keyword": prod["keyword"],
                        "clients": self.company_client,
                    },
                    {
                        "$push": {
                            "product_order": {
                                "product_id": prod["product_id"],
                                "product_rank": prod["product_rank"],
                            }
                        }
                    },
                    upsert=True,
                )
                spider.success_counts["added"] += 1
                self.db[self.data_collection_name].find_one_and_update(
                    {
                        "product_id": prod["product_id"],
                    },
                    {
                        "$set": {
                            "product_name": prod["product_name"],
                            "product_brand": prod["product_brand"],
                            "product_original_price": prod["product_original_price"],
                            # "product_categories": prod["product_categories"],
                            "product_sale_price": prod["product_sale_price"],
                            "product_rating": prod["product_rating"],
                            "product_total_ratings": prod["product_total_ratings"],
                            "product_stock": prod["product_stock"],
                            "product_discount": prod["product_discount"],
                            "product_availability": prod["product_availability"],
                        }
                    },
                    upsert=True,
                )
        return item

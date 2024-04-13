from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor, defer

# from amazon_product_scraping.spiders.AmazonTop100BSRSpider import AmazonTop100BSRSpider
from amazon_product_scraping.spiders.ShopeeBrandSearchListSpider import (
    ShopeeBrandSearchListSpider,
)
from amazon_product_scraping.spiders.ShopeeProductInfoSpider import (
    ShopeeProductInfoSpider,
)
from amazon_product_scraping.spiders.ShopeeProductDailyMoveSpider import (
    ShopeeProductDailyMoveSpider,
)
from amazon_product_scraping.spiders.ShopeeProductCommentsSpider import (
    ShopeeProductCommentsSpider,
)
from amazon_product_scraping.spiders.ShopeeShareOfSearchSpider import (
    ShopeeShareOfSearchSpider,
)
from scrapy.utils.project import get_project_settings
import datetime
from telegram_notifier import TelegramNotifier


settings = get_project_settings()
process = CrawlerRunner(settings=settings)


@defer.inlineCallbacks
def run(
    search_list: bool = False,
    brand_search: bool = False,
    force_info_scrape: bool = False,
    daily_move: bool = False,
    comments: bool = False,
    sos: bool = False,
    mongo_db: str = "shopee_marketplace_scraping",
    run_summary_file: str = "run_summary",
    company_client: str = "WTC",
    search_list_links: list = [],
    brand_search_ids: list = [],
    sos_keywords: list = [],
):
    time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    telegram = TelegramNotifier()

    with open("{}.log".format(run_summary_file), "a") as f:
        f.write("\n********************\n")
        f.write("Scraper started : {}\n\n".format(time))
    telegram.send_message(
        "Hi,\nScraper Started: {}\nScraper: {} saving to DB {}".format(
            time, run_summary_file, mongo_db
        )
    )

    new_products_added = 0

    # if search_list:
    #     # # Search List Spider
    #     failed_urls = []
    #     cold_run = True
    #     total_success_counts = {
    #         'new': 0,
    #         'existing': 0
    #     }
    #     for i in range(20):
    #         success_counts = {
    #             'new': 0,
    #             'existing': 0
    #         }
    #         yield process.crawl(
    #             ShopeeSearchListSpider,
    #             start_urls = search_list_links,
    #             cold_run = cold_run,
    #             failed_urls = failed_urls,
    #             company_client = company_client,
    #             mongo_db = mongo_db,
    #             success_counts = success_counts
    #         )
    #         cold_run = False
    #         total_success_counts['new'] += success_counts['new']
    #         total_success_counts['existing'] += success_counts['existing']
    #         with open('{}.log'.format(run_summary_file), 'a') as f:
    #             f.write("Run {}:\nFound {} products in ShopeeSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(i+1, success_counts['new']+success_counts['existing'], success_counts['new'], success_counts['existing']))
    #         if len(failed_urls) == 0:
    #             break

    #     telegram_message = ""
    #     with open('{}.log'.format(run_summary_file), 'a') as f:
    #         f.write("ShopeeSearchListSpider Summary:\nFound {} products in ShopeeSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(total_success_counts['new']+total_success_counts['existing'], total_success_counts['new'], total_success_counts['existing']))
    #         telegram_message += "ShopeeSearchListSpider Summary:\nFound {} products in ShopeeSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(total_success_counts['new']+total_success_counts['existing'], total_success_counts['new'], total_success_counts['existing'])
    #         f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
    #         telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
    #         for j in range(len(failed_urls)):
    #             f.write("\t\t{}. {}\n".format(j+1, failed_urls[j]))
    #             telegram_message += "\t\t{}. {}\n".format(j+1, failed_urls[j])
    #         f.write("\n")
    #     telegram.send_message(telegram_message)
    #     new_products_added += total_success_counts['new']

    if brand_search:
        # # Brand Search List Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "existing": 0}
        for i in range(20):
            success_counts = {"new": 0, "existing": 0}
            yield process.crawl(
                ShopeeBrandSearchListSpider,
                # start_urls = brand_search_links,
                brand_ids=brand_search_ids,
                cold_run=cold_run,
                failed_urls=failed_urls,
                company_client=company_client,
                mongo_db=mongo_db,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["new"] += success_counts["new"]
            total_success_counts["existing"] += success_counts["existing"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound {} products in ShopeeBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(
                        i + 1,
                        success_counts["new"] + success_counts["existing"],
                        success_counts["new"],
                        success_counts["existing"],
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "ShopeeBrandSearchListSpider Summary:\nFound {} products in ShopeeBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                    total_success_counts["new"] + total_success_counts["existing"],
                    total_success_counts["new"],
                    total_success_counts["existing"],
                )
            )
            telegram_message += "ShopeeBrandSearchListSpider Summary:\nFound {} products in ShopeeBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                total_success_counts["new"] + total_success_counts["existing"],
                total_success_counts["new"],
                total_success_counts["existing"],
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)
        new_products_added += total_success_counts["new"]

    if force_info_scrape or ((search_list or brand_search) and new_products_added > 0):
        # # Product Info Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(20):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                ShopeeProductInfoSpider,
                force_info_scrape=force_info_scrape,
                cold_run=cold_run,
                failed_urls=failed_urls,
                mongo_db=mongo_db,
                success_counts=success_counts,
                time=time,
            )
            cold_run = False
            total_success_counts["new"] += success_counts["new"]
            total_success_counts["added"] += success_counts["added"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound {} products in ShopeeProductInfoSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "ShopeeProductInfoSpider Summary:\nFound {} products in ShopeeProductInfoSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "ShopeeProductInfoSpider Summary:\nFound {} products in ShopeeProductInfoSpider\n\t- {} successfully added to DB\n".format(
                total_success_counts["new"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if daily_move:
        # # Product Sale Price BSR Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(20):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                ShopeeProductDailyMoveSpider,
                time=time,
                cold_run=cold_run,
                failed_urls=failed_urls,
                mongo_db=mongo_db,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["new"] += success_counts["new"]
            total_success_counts["added"] += success_counts["added"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound {} products in ShopeeProductDailyMoveSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "ShopeeProductDailyMoveSpider Summary:\nFound {} products in ShopeeProductDailyMoveSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "ShopeeProductDailyMoveSpider Summary:\nFound {} products in ShopeeProductDailyMoveSpider\n\t- {} successfully added to DB\n".format(
                total_success_counts["new"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if comments:
        # # Product Comments Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {
            "prods_checked": 0,
            "prods_with_new_comms": 0,
            "new_comments": 0,
        }
        for i in range(1):
            success_counts = {
                "prods_checked": 0,
                "prods_with_new_comms": 0,
                "new_comments": 0,
            }
            yield process.crawl(
                ShopeeProductCommentsSpider,
                cold_run=cold_run,
                failed_urls=failed_urls,
                count=50,
                time=time,
                mongo_db=mongo_db,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["prods_checked"] += success_counts["prods_checked"]
            total_success_counts["prods_with_new_comms"] += success_counts[
                "prods_with_new_comms"
            ]
            total_success_counts["new_comments"] += success_counts["new_comments"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nChecked {} products in ShopeeProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
                        i + 1,
                        success_counts["prods_checked"],
                        success_counts["prods_with_new_comms"],
                        success_counts["new_comments"],
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "ShopeeProductCommentsSpider Summary:\nChecked {} products in ShopeeProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
                    total_success_counts["prods_checked"],
                    total_success_counts["prods_with_new_comms"],
                    total_success_counts["new_comments"],
                )
            )
            telegram_message += "ShopeeProductCommentsSpider Summary:\nChecked {} products in ShopeeProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
                total_success_counts["prods_checked"],
                total_success_counts["prods_with_new_comms"],
                total_success_counts["new_comments"],
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if sos:
        # # Share of Search Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"added": 0, "ranked": 0}
        for i in range(20):
            success_counts = {"added": 0, "ranked": 0}
            yield process.crawl(
                ShopeeShareOfSearchSpider,
                time=time,
                cold_run=cold_run,
                failed_urls=failed_urls,
                mongo_db=mongo_db,
                company_client=company_client,
                keywords=sos_keywords,
                pages=1,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["added"] += success_counts["added"]
            total_success_counts["ranked"] += success_counts["ranked"]
            print(failed_urls)
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound and Added rank of {} products in ShopeeShareOfSearchSpider\n\t- {} were new products\n\t- {} URLs failed\n\n".format(
                        i + 1,
                        success_counts["ranked"],
                        success_counts["added"],
                        len(failed_urls),
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "ShopeeShareOfSearchSpider Summary:\nFound and Added rank of {} products in ShopeeShareOfSearchSpider\n\t- {} were new products\n".format(
                    total_success_counts["ranked"], total_success_counts["added"]
                )
            )
            telegram_message += "ShopeeShareOfSearchSpider Summary:\nFound and Added rank of {} products in ShopeeShareOfSearchSpider\n\t- {} were new products\n".format(
                total_success_counts["ranked"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    reactor.stop()


if __name__ == "__main__":
    # keywords = pd.read_csv('Havells-Keywords.csv', header=None)
    # keywords = keywords[0].to_list()
    # print(keywords)
    keywords = []
    run(
        search_list=False,
        brand_search=True,
        force_info_scrape=False,
        daily_move=True,
        comments=False,
        sos=False,
        mongo_db="shopee_marketplace_scraping_WTC",
        run_summary_file="run_summary_shopee_WTC",
        company_client="WTC",
        brand_search_ids=[
            ["14318452", "3160322"],
            ["11487927", "2125929"],
            ["14318452", "46997891"],
            ["62579622", "18455550"],
            ["11487927", "2149949"],
        ],
        sos_keywords=keywords,
    )
    reactor.run()

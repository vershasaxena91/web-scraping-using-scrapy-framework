from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor, defer
from amazon_product_scraping.spiders.FlipkartProductRankSpider import (
    FlipkartProductRankSpider,
)
from amazon_product_scraping.spiders.FlipkartSearchListSpider import (
    FlipkartSearchListSpider,
)
from amazon_product_scraping.spiders.FlipkartProductInfoSpider import (
    FlipkartProductInfoSpider,
)
from amazon_product_scraping.spiders.FlipkartProductSalePriceSpider import (
    FlipkartProductSalePriceSpider,
)
from amazon_product_scraping.spiders.FlipkartShareOfSearchSpider import (
    FlipkartShareOfSearchSpider,
)
from amazon_product_scraping.spiders.FlipkartProductCommentsSpider import (
    FlipkartProductCommentsSpider,
)
from scrapy.utils.project import get_project_settings
import datetime
from telegram_notifier import TelegramNotifier


settings = get_project_settings()
process = CrawlerRunner(settings=settings)


@defer.inlineCallbacks
def run(
    rank_move: bool = False,
    insert_at_rank: bool = False,
    search_list: bool = False,
    force_info_scrape: bool = False,
    price_bsr_move: bool = False,
    comments: bool = False,
    sos: bool = False,
    mongo_db: str = "amazon_marketplace_scraping",
    run_summary_file: str = "run_summary",
    company_client: str = "WTC",
    rank_list_links: list = [],
    search_list_links: list = [],
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
    if rank_move:
        # # Rank Move Spider
        failed_urls = []
        prod_ranks = dict()
        cold_run = True
        total_success_counts = {"found": 0, "new": 0, "added": 0}
        for i in range(10):
            success_counts = {"found": 0, "new": 0, "added": 0}
            yield process.crawl(
                FlipkartProductRankSpider,
                time=time,
                start_urls=rank_list_links,
                cold_run=cold_run,
                failed_urls=failed_urls,
                company_client=company_client,
                pages=25,
                mongo_db=mongo_db,
                upsert=insert_at_rank,
                prods=prod_ranks,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["found"] += success_counts["found"]
            total_success_counts["new"] += success_counts["new"]
            total_success_counts["added"] += success_counts["added"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound {} products in FlipkartProductRankSpider\n\t- {} new products were added to DB\n\t- {} ranks were added to DB\n\n".format(
                        i + 1,
                        success_counts["found"],
                        success_counts["new"],
                        success_counts["added"],
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "FlipkartProductRankSpider Summary:\nFound {} products in FlipkartProductRankSpider\n\t- {} new products were added to DB\n\t- {} ranks were added to DB\n".format(
                    total_success_counts["found"],
                    total_success_counts["new"],
                    total_success_counts["added"],
                )
            )
            telegram_message += "FlipkartProductRankSpider Summary:\nFound {} products in FlipkartProductRankSpider\n\t- {} new products were added to DB\n\t- {} ranks were added to DB\n".format(
                total_success_counts["found"],
                total_success_counts["new"],
                total_success_counts["added"],
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)
        new_products_added += total_success_counts["new"]

    if search_list:
        # # Search List Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "existing": 0}
        for i in range(10):
            success_counts = {"new": 0, "existing": 0}
            yield process.crawl(
                FlipkartSearchListSpider,
                start_urls=search_list_links,
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
                    "Run {}:\nFound {} products in FlipkartSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(
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
                "FlipkartSearchListSpider Summary:\nFound {} products in FlipkartSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                    total_success_counts["new"] + total_success_counts["existing"],
                    total_success_counts["new"],
                    total_success_counts["existing"],
                )
            )
            telegram_message += "FlipkartSearchListSpider Summary:\nFound {} products in FlipkartSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
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

    if force_info_scrape or (
        (insert_at_rank or search_list) and new_products_added > 0
    ):
        # # Product Info Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(10):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                FlipkartProductInfoSpider,
                force_info_scrape=force_info_scrape,
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
                    "Run {}:\nFound {} products in FlipkartProductInfoSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "FlipkartProductInfoSpider Summary:\nFound {} products in FlipkartProductInfoSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "FlipkartProductInfoSpider Summary:\nFound {} products in FlipkartProductInfoSpider\n\t- {} successfully added to DB\n".format(
                total_success_counts["new"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if price_bsr_move:
        # # Product Sale Price BSR Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(10):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                FlipkartProductSalePriceSpider,
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
                    "Run {}:\nFound {} products in FlipkartProductSalePriceSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "FlipkartProductSalePriceSpider Summary:\nFound {} products in FlipkartProductSalePriceSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "FlipkartProductSalePriceSpider Summary:\nFound {} products in FlipkartProductSalePriceSpider\n\t- {} successfully added to DB\n".format(
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
        for i in range(10):
            success_counts = {
                "prods_checked": 0,
                "prods_with_new_comms": 0,
                "new_comments": 0,
            }
            yield process.crawl(
                FlipkartProductCommentsSpider,
                cold_run=cold_run,
                failed_urls=failed_urls,
                count=10,
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
                    "Run {}:\nChecked {} products in FlipkartProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
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
                "FlipkartProductCommentsSpider Summary:\nChecked {} products in FlipkartProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
                    total_success_counts["prods_checked"],
                    total_success_counts["prods_with_new_comms"],
                    total_success_counts["new_comments"],
                )
            )
            telegram_message += "FlipkartProductCommentsSpider Summary:\nChecked {} products in FlipkartProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
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
        for i in range(10):
            success_counts = {"added": 0, "ranked": 0}
            yield process.crawl(
                FlipkartShareOfSearchSpider,
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
                    "Run {}:\nFound and Added rank of {} products in FlipkartShareOfSearchSpider\n\t- {} were new products\n\t- {} URLs failed\n\n".format(
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
                "FlipkartShareOfSearchSpider Summary:\nFound and Added rank of {} products in FlipkartShareOfSearchSpider\n\t- {} were new products\n".format(
                    total_success_counts["ranked"], total_success_counts["added"]
                )
            )
            telegram_message += "FlipkartShareOfSearchSpider Summary:\nFound and Added rank of {} products in FlipkartShareOfSearchSpider\n\t- {} were new products\n".format(
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

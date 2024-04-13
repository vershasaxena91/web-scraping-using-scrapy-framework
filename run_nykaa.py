from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor, defer

from amazon_product_scraping.spiders.NykaaBrandSearchListSpider import (
    NykaaBrandSearchListSpider,
)
from amazon_product_scraping.spiders.NykaaProductInfoSpider import (
    NykaaProductInfoSpider,
)
from amazon_product_scraping.spiders.NykaaProductSalePriceSpider import (
    NykaaProductSalePriceSpider,
)
from amazon_product_scraping.spiders.NykaaProductRankSpider import (
    NykaaProductRankSpider,
)
from amazon_product_scraping.spiders.NykaaProductCommentsSpider import (
    NykaaProductCommentsSpider,
)
from amazon_product_scraping.spiders.NykaaProductQuestionsSpider import (
    NykaaProductQuestionsSpider,
)

from amazon_product_scraping.spiders.NykaaShareOfSearchSpider import (
    NykaaShareOfSearchSpider,
)
from scrapy.utils.project import get_project_settings
import datetime
from telegram_notifier import TelegramNotifier


settings = get_project_settings()
process = CrawlerRunner(settings=settings)


@defer.inlineCallbacks
def run(
    brand_search: bool = False,
    force_info_scrape: bool = False,
    price_move: bool = False,
    rank_move: bool = False,
    comments: bool = False,
    questions: bool = False,
    sos: bool = False,
    mongo_db: str = "nykaa_marketplace_scraping",
    run_summary_file: str = "run_summary",
    company_client: str = "WTC",
    brand_search_ids: list = [],
    rank_list_links: list = [],
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

    if brand_search:
        # # Brand Search List Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "existing": 0}
        for i in range(5):
            success_counts = {"new": 0, "existing": 0}
            yield process.crawl(
                NykaaBrandSearchListSpider,
                # start_urls = brand_search_links,
                brand_ids=brand_search_ids,
                cold_run=cold_run,
                failed_urls=failed_urls,
                company_client=company_client,
                mongo_db=mongo_db,
                success_counts=success_counts,
                time=time,
            )
            cold_run = False
            total_success_counts["new"] += success_counts["new"]
            total_success_counts["existing"] += success_counts["existing"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound {} products in NykaaBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(
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
                "NykaaBrandSearchListSpider Summary:\nFound {} products in NykaaBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                    total_success_counts["new"] + total_success_counts["existing"],
                    total_success_counts["new"],
                    total_success_counts["existing"],
                )
            )
            telegram_message += "NykaaBrandSearchListSpider Summary:\nFound {} products in NykaaBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
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

    if force_info_scrape:
        # # Product Info Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(5):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                NykaaProductInfoSpider,
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
                    "Run {}:\nFound {} products in NykaaProductInfoSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "NykaaProductInfoSpider Summary:\nFound {} products in NykaaProductInfoSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "NykaaProductInfoSpider Summary:\nFound {} products in NykaaProductInfoSpider\n\t- {} successfully added to DB\n".format(
                total_success_counts["new"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if price_move:
        # # Product Sale Price BSR Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(5):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                NykaaProductSalePriceSpider,
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
                    "Run {}:\nFound {} products in NykaaProductSalePriceSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "NykaaProductSalePriceSpider Summary:\nFound {} products in NykaaProductSalePriceSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "NykaaProductSalePriceSpider Summary:\nFound {} products in NykaaProductSalePriceSpider\n\t- {} successfully added to DB\n".format(
                total_success_counts["new"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if rank_move:
        # # Rank Move Spider
        failed_urls = []
        prod_ranks = dict()
        cold_run = True
        total_success_counts = {"found": 0, "new": 0, "added": 0}
        for i in range(5):
            success_counts = {"found": 0, "new": 0, "added": 0}
            yield process.crawl(
                NykaaProductRankSpider,
                time=time,
                start_urls=rank_list_links,
                cold_run=cold_run,
                failed_urls=failed_urls,
                company_client=company_client,
                pages=50,
                mongo_db=mongo_db,
                prods=prod_ranks,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["found"] += success_counts["found"]
            total_success_counts["new"] += success_counts["new"]
            total_success_counts["added"] += success_counts["added"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound {} products in NykaaProductRankSpider\n\t- {} new products were added to DB\n\t- {} ranks were added to DB\n\n".format(
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
                "NykaaProductRankSpider Summary:\nFound {} products in NykaaProductRankSpider\n\t- {} new products were added to DB\n\t- {} ranks were added to DB\n".format(
                    total_success_counts["found"],
                    total_success_counts["new"],
                    total_success_counts["added"],
                )
            )
            telegram_message += "NykaaProductRankSpider Summary:\nFound {} products in NykaaProductRankSpider\n\t- {} new products were added to DB\n\t- {} ranks were added to DB\n".format(
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
                NykaaProductCommentsSpider,
                cold_run=cold_run,
                failed_urls=failed_urls,
                count=20,
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
                    "Run {}:\nChecked {} products in NykaaProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
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
                "NykaaProductCommentsSpider Summary:\nChecked {} products in NykaaProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
                    total_success_counts["prods_checked"],
                    total_success_counts["prods_with_new_comms"],
                    total_success_counts["new_comments"],
                )
            )
            telegram_message += "NykaaProductCommentsSpider Summary:\nChecked {} products in NykaaProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
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

    if questions:
        # # Product Questions Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {
            "prods_checked": 0,
            "prods_with_new_ques": 0,
            "new_questions": 0,
        }
        for i in range(1):
            success_counts = {
                "prods_checked": 0,
                "prods_with_new_ques": 0,
                "new_questions": 0,
            }
            yield process.crawl(
                NykaaProductQuestionsSpider,
                cold_run=cold_run,
                failed_urls=failed_urls,
                count=5,
                time=time,
                mongo_db=mongo_db,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["prods_checked"] += success_counts["prods_checked"]
            total_success_counts["prods_with_new_ques"] += success_counts[
                "prods_with_new_ques"
            ]
            total_success_counts["new_questions"] += success_counts["new_questions"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nChecked {} products in NykaaProductQuestionsSpider\n\t- {} products had new questions\n\t- {} questions successfully added to DB\n\n".format(
                        i + 1,
                        success_counts["prods_checked"],
                        success_counts["prods_with_new_ques"],
                        success_counts["new_questions"],
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "NykaaProductQuestionsSpider Summary:\nChecked {} products in NykaaProductQuestionsSpider\n\t- {} products had new questions\n\t- {} questions successfully added to DB\n\n".format(
                    total_success_counts["prods_checked"],
                    total_success_counts["prods_with_new_ques"],
                    total_success_counts["new_questions"],
                )
            )
            telegram_message += "NykaaProductQuestionsSpider Summary:\nChecked {} products in NykaaProductQuestionsSpider\n\t- {} products had new questions\n\t- {} questions successfully added to DB\n\n".format(
                total_success_counts["prods_checked"],
                total_success_counts["prods_with_new_ques"],
                total_success_counts["new_questions"],
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
        for i in range(1):
            success_counts = {"added": 0, "ranked": 0}
            yield process.crawl(
                NykaaShareOfSearchSpider,
                time=time,
                cold_run=cold_run,
                failed_urls=failed_urls,
                mongo_db=mongo_db,
                company_client=company_client,
                keywords=sos_keywords,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["added"] += success_counts["added"]
            total_success_counts["ranked"] += success_counts["ranked"]
            print(failed_urls)
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nFound and Added rank of {} products in NykaaShareOfSearchSpider\n\t- {} were new products\n\t- {} URLs failed\n\n".format(
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
                "NykaaShareOfSearchSpider Summary:\nFound and Added rank of {} products in NykaaShareOfSearchSpider\n\t- {} were new products\n".format(
                    total_success_counts["ranked"], total_success_counts["added"]
                )
            )
            telegram_message += "NykaaShareOfSearchSpider Summary:\nFound and Added rank of {} products in NykaaShareOfSearchSpider\n\t- {} were new products\n".format(
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

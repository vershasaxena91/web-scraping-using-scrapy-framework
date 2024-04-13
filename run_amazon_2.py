from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import reactor, defer
from amazon_product_scraping.spiders.AmazonTop100BSRSpider import AmazonTop100BSRSpider
from amazon_product_scraping.spiders.AmazonSearchListSpider import (
    AmazonSearchListSpider,
)
from amazon_product_scraping.spiders.AmazonProductInfoSpider import (
    AmazonProductInfoSpider,
)
from amazon_product_scraping.spiders.AmazonProductSalePriceBSRSpider import (
    AmazonProductSalePriceBSRSpider,
)
from amazon_product_scraping.spiders.AmazonProductTotalQuestionsSpider import (
    AmazonProductTotalQuestionsSpider,
)
from amazon_product_scraping.spiders.AmazonProductCommentsSpider import (
    AmazonProductCommentsSpider,
)
from amazon_product_scraping.spiders.AmazonProductQuestionsSpider import (
    AmazonProductQuestionsSpider,
)
from amazon_product_scraping.spiders.AmazonShareOfSearchSpider import (
    AmazonShareOfSearchSpider,
)
from amazon_product_scraping.spiders.AmazonBrandSearchListSpider import (
    AmazonBrandSearchListSpider,
)
from scrapy.utils.project import get_project_settings
import datetime
from telegram_notifier import TelegramNotifier


settings = get_project_settings()
process = CrawlerRunner(settings=settings)


@defer.inlineCallbacks
def run(
    bsr_100: bool = False,
    search_list: bool = False,
    brand_search: bool = False,
    force_info_scrape: bool = False,
    price_bsr_move: bool = False,
    total_questions_move: bool = False,
    comments: bool = False,
    questionAnswers: bool = False,
    sos: bool = False,
    mongo_db: str = "amazon_marketplace_scraping",
    run_summary_file: str = "run_summary",
    company_client: str = "WTC",
    bsr_100_links: list = [],
    search_list_links: list = [],
    brand_search_links: list = [],
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
    if bsr_100:
        # # Top 100 BSR Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "existing": 0}
        for i in range(20):
            success_counts = {"new": 0, "existing": 0}
            yield process.crawl(
                AmazonTop100BSRSpider,
                start_urls=bsr_100_links,
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
                    "Run {}:\nFound {} products in AmazonTop100BSRSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(
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
                "AmazonTop100BSRSpider Summary:\nFound {} products in AmazonTop100BSRSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                    total_success_counts["new"] + total_success_counts["existing"],
                    total_success_counts["new"],
                    total_success_counts["existing"],
                )
            )
            telegram_message += "AmazonTop100BSRSpider Summary:\nFound {} products in AmazonTop100BSRSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
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

    if search_list:
        # # Search List Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "existing": 0}
        for i in range(20):
            success_counts = {"new": 0, "existing": 0}
            yield process.crawl(
                AmazonSearchListSpider,
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
                    "Run {}:\nFound {} products in AmazonSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(
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
                "AmazonSearchListSpider Summary:\nFound {} products in AmazonSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                    total_success_counts["new"] + total_success_counts["existing"],
                    total_success_counts["new"],
                    total_success_counts["existing"],
                )
            )
            telegram_message += "AmazonSearchListSpider Summary:\nFound {} products in AmazonSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
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

    if brand_search:
        # # Brand Search List Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "existing": 0}
        for i in range(20):
            success_counts = {"new": 0, "existing": 0}
            yield process.crawl(
                AmazonBrandSearchListSpider,
                start_urls=brand_search_links,
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
                    "Run {}:\nFound {} products in AmazonBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n\n".format(
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
                "AmazonBrandSearchListSpider Summary:\nFound {} products in AmazonBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
                    total_success_counts["new"] + total_success_counts["existing"],
                    total_success_counts["new"],
                    total_success_counts["existing"],
                )
            )
            telegram_message += "AmazonBrandSearchListSpider Summary:\nFound {} products in AmazonBrandSearchListSpider\n\t- {} new products were added to DB\n\t- {} products were already in DB\n".format(
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
        (bsr_100 or search_list or brand_search) and new_products_added > 0
    ):
        # # Product Info Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(20):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                AmazonProductInfoSpider,
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
                    "Run {}:\nFound {} products in AmazonProductInfoSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "AmazonProductInfoSpider Summary:\nFound {} products in AmazonProductInfoSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "AmazonProductInfoSpider Summary:\nFound {} products in AmazonProductInfoSpider\n\t- {} successfully added to DB\n".format(
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
        for i in range(20):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                AmazonProductSalePriceBSRSpider,
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
                    "Run {}:\nFound {} products in AmazonProductSalePriceBSRSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "AmazonProductSalePriceBSRSpider Summary:\nFound {} products in AmazonProductSalePriceBSRSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "AmazonProductSalePriceBSRSpider Summary:\nFound {} products in AmazonProductSalePriceBSRSpider\n\t- {} successfully added to DB\n".format(
                total_success_counts["new"], total_success_counts["added"]
            )
            f.write("\tFailed URLs: {}\n".format(len(failed_urls)))
            telegram_message += "\tFailed URLs: {}\n".format(len(failed_urls))
            for j in range(len(failed_urls)):
                f.write("\t\t{}. {}\n".format(j + 1, failed_urls[j]))
                telegram_message += "\t\t{}. {}\n".format(j + 1, failed_urls[j])
            f.write("\n")
        telegram.send_message(telegram_message)

    if total_questions_move:
        # # Product Sale Price BSR Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {"new": 0, "added": 0}
        for i in range(20):
            success_counts = {"new": 0, "added": 0}
            yield process.crawl(
                AmazonProductTotalQuestionsSpider,
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
                    "Run {}:\nFound {} products in AmazonProductTotalQuestionsSpider\n\t- {} successfully added to DB\n\n".format(
                        i + 1, success_counts["new"], success_counts["added"]
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "AmazonProductTotalQuestionsSpider Summary:\nFound {} products in AmazonProductTotalQuestionsSpider\n\t- {} successfully added to DB\n".format(
                    total_success_counts["new"], total_success_counts["added"]
                )
            )
            telegram_message += "AmazonProductTotalQuestionsSpider Summary:\nFound {} products in AmazonProductTotalQuestionsSpider\n\t- {} successfully added to DB\n".format(
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
        for i in range(5):
            success_counts = {
                "prods_checked": 0,
                "prods_with_new_comms": 0,
                "new_comments": 0,
            }
            yield process.crawl(
                AmazonProductCommentsSpider,
                cold_run=cold_run,
                failed_urls=failed_urls,
                count=10,
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
                    "Run {}:\nChecked {} products in AmazonProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
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
                "AmazonProductCommentsSpider Summary:\nChecked {} products in AmazonProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
                    total_success_counts["prods_checked"],
                    total_success_counts["prods_with_new_comms"],
                    total_success_counts["new_comments"],
                )
            )
            telegram_message += "AmazonProductCommentsSpider Summary:\nChecked {} products in AmazonProductCommentsSpider\n\t- {} products had new comments\n\t- {} comments successfully added to DB\n\n".format(
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

    if questionAnswers:
        # # Product Questions Spider
        failed_urls = []
        cold_run = True
        total_success_counts = {
            "prods_checked": 0,
            "prods_with_new_QAs": 0,
            "new_QAs": 0,
        }
        for i in range(5):
            success_counts = {"prods_checked": 0, "prods_with_new_QAs": 0, "new_QAs": 0}
            yield process.crawl(
                AmazonProductQuestionsSpider,
                cold_run=cold_run,
                failed_urls=failed_urls,
                count=10,
                mongo_db=mongo_db,
                success_counts=success_counts,
            )
            cold_run = False
            total_success_counts["prods_checked"] += success_counts["prods_checked"]
            total_success_counts["prods_with_new_QAs"] += success_counts[
                "prods_with_new_QAs"
            ]
            total_success_counts["new_QAs"] += success_counts["new_QAs"]
            with open("{}.log".format(run_summary_file), "a") as f:
                f.write(
                    "Run {}:\nChecked {} products in AmazonProductQuestionsSpider\n\t- {} products had new questions\n\t- {} questions successfully added to DB\n\n".format(
                        i + 1,
                        success_counts["prods_checked"],
                        success_counts["prods_with_new_QAs"],
                        success_counts["new_QAs"],
                    )
                )
            if len(failed_urls) == 0:
                break

        telegram_message = ""
        with open("{}.log".format(run_summary_file), "a") as f:
            f.write(
                "AmazonProductQuestionsSpider Summary:\nChecked {} products in AmazonProductQuestionsSpider\n\t- {} products had new questions\n\t- {} questions successfully added to DB\n\n".format(
                    total_success_counts["prods_checked"],
                    total_success_counts["prods_with_new_QAs"],
                    total_success_counts["new_QAs"],
                )
            )
            telegram_message += "AmazonProductQuestionsSpider Summary:\nChecked {} products in AmazonProductQuestionsSpider\n\t- {} products had new questions\n\t- {} questions successfully added to DB\n\n".format(
                total_success_counts["prods_checked"],
                total_success_counts["prods_with_new_QAs"],
                total_success_counts["new_QAs"],
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
                AmazonShareOfSearchSpider,
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
                    "Run {}:\nFound and Added rank of {} products in AmazonShareOfSearchSpider\n\t- {} were new products\n\t- {} URLs failed\n\n".format(
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
                "AmazonShareOfSearchSpider Summary:\nFound and Added rank of {} products in AmazonShareOfSearchSpider\n\t- {} were new products\n".format(
                    total_success_counts["ranked"], total_success_counts["added"]
                )
            )
            telegram_message += "AmazonShareOfSearchSpider Summary:\nFound and Added rank of {} products in AmazonShareOfSearchSpider\n\t- {} were new products\n".format(
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

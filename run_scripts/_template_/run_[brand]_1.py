import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("[Brand]-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=False,
        comments=False,
        questionAnswers=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_[brand]",
        run_summary_file="run_summary_[brand]",
        company_client="[Brand]",
        bsr_100_links=[],
        search_list_links=[],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

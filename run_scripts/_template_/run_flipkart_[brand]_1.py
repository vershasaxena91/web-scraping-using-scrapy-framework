import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("[Brand]-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        rank_move=False,
        insert_at_rank=False,
        search_list=False,
        force_info_scrape=False,
        price_bsr_move=False,
        comments=False,
        sos=False,
        mongo_db="flipkart_marketplace_scraping_[brand]",
        run_summary_file="run_summary_flipkart_[brand]",
        company_client="[Brand]",
        rank_list_links=[],
        search_list_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

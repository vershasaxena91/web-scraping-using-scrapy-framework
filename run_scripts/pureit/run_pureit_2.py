import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Pureit-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=False,
        total_questions_move=False,
        comments=True,
        questionAnswers=True,
        sos=False,
        mongo_db="amazon_marketplace_scraping_pureit",
        run_summary_file="run_summary_pureit",
        company_client="PureIt",
        search_list_links=["https://tinyurl.com/2tk4brt3"],
        sos_keywords=keywords,
    )
    reactor.run()

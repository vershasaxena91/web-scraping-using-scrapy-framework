import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nescafe-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=True,
        total_questions_move=True,
        comments=False,
        questionAnswers=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_nescafe_coffee_maker",
        run_summary_file="run_summary_nescafe",
        company_client="Nescafe",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=Nescafe+E+Smart+Coffee+Maker.&rh=n%3A1379960031&ref=nb_sb_noss"
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

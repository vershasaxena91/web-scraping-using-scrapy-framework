import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nescafe_sunrise-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_nescafe_sunrise",
        run_summary_file="run_summary_nescafe_sunrise",
        company_client="Nescafe_sunrise",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=coffee&i=grocery&rh=n%3A2454178031%2Cp_89%3ABru%7CNescaf%C3%A9%7CRage+Coffee%7CSleepy+Owl%7CTata&dc&qid=1649674615&rnid=3837712031&sprefix=coff%2Caps%2C476&ref=sr_nr_p_89_10"
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

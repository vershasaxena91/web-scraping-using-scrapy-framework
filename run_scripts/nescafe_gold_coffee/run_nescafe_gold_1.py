import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nescafe_Gold-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_nescafe_gold",
        run_summary_file="run_summary_nescafe_gold",
        company_client="Nescafe_Gold",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?i=grocery&bbn=4860057031&rh=n%3A4860057031%2Cp_89%3ABru%7CNESCAF%C3%89+GOLD%7CNescafe+Gold%7CNescaf%C3%A9%7CTata+Coffee+Grand&dc&qid=1649407832&rnid=3837712031&ref=sr_nr_p_89_5"
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

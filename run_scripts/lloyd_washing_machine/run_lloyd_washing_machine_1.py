import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Lloyd_Washing_Machine-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_lloyd_washing_machine",
        run_summary_file="run_summary_lloyd_washing_machine",
        company_client="Lloyd_Washing_Machine",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?bbn=1380369031&rh=n%3A1380369031%2Cp_89%3ASamsung&dc&qid=1656909959&rnid=3837712031&ref=lp_1380369031_nr_p_89_0",  # Samsung
            "https://www.amazon.in/s?i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_89%3AHaier&dc&qid=1656910151&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AuWwSjAuKk%2Fx1hjagxTrfaTDkoj6pBF0ep9npViKSxhU",  # Haier
            "https://www.amazon.in/s?i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_89%3ABosch&dc&qid=1656910125&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3A1ru4cbHwKE7k7BJaS85yQZ%2BKLa8m9iSjeWf1DX3cqFk",  # Bosch
            "https://www.amazon.in/s?i=kitchen&bbn=1380369031&rh=n%3A1380369031%2Cp_89%3AWhirlpool&dc&qid=1656910078&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AZKt8kGjs6Xoaob90chgabydYE7beXBfQNHAggzFlDjE",  # Whirlpool
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

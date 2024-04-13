import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Park_Avenue-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_park_avenue",
        run_summary_file="run_summary_park_avenue",
        company_client="Park_Avenue",
        bsr_100_links=[],
        search_list_links=[
            # "https://tinyurl.com/2p98f5w8", # https://www.amazon.in/s?k=deodorants&i=beauty&rh=n%3A1355016031%2Cp_89%3AAXE%7CBrut%7CFOGG%7CNivea%7CSet+Wet%7CYardley&dc&crid=1LG3W95R63MTC&qid=1647530440&rnid=3837712031&sprefix=deodorants%2Caps%2C180&ref=sr_nr_p_89_9
            "https://tinyurl.com/y6uz2cv7",  # https://www.amazon.in/s?k=deodorants&i=beauty&rh=n%3A1355016031%2Cp_89%3AAXE%7CBrut%7CFOGG%7CNivea%7CPark+Avenue%7CSet+Wet%7CYardley&dc&crid=1LG3W95R63MTC&qid=1647530440&rnid=3837712031&sprefix=deodorants%2Caps%2C180&ref=sr_nr_p_89_9
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

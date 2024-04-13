import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nestle_cornflakes-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_nestle_cornflakes",
        run_summary_file="run_summary_nestle_cornflakes",
        company_client="Nestle_cornflakes",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=Nestle+Gold+cornflakes+%26+Koko+Crunch",
            # "https://www.amazon.in/s?k=Nestle+Gold+cornflakes+%26+Koko+Crunch&rh=n%3A2454178031%2Cp_89%3ANestle&dc&crid=2R4FIUHUZUVGV&qid=1649675730&rnid=3837712031&sprefix=nestle+gold+cornflakes+%26+koko+crunch+%2Caps%2C276&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=cornflakes&i=grocery&rh=n%3A2454178031%2Cp_89%3AKellogg%27s%7CTrue+Elements%7CYogabar&dc&crid=3J1TUZU6OVJ9D&qid=1649675537&rnid=3837712031&sprefix=cornflakes%2Caps%2C861&ref=sr_nr_p_89_6",
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

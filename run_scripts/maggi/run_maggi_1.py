import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Maggi-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_maggi",
        run_summary_file="run_summary_maggi",
        company_client="Maggi",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=noodles&i=grocery&rh=n%3A2454178031%2Cp_89%3ACHING%27S%7CChing%27s+Secret%7CChings%7CMaggi%7CNestl%C3%A9%7CSlurrp+Farm%7CSunfeast+YiPPee%21%7CTop+ramen%7CWai+Wai&dc&crid=2IYBSOR4BSMK5&qid=1649671904&rnid=3837712031&sprefix=noodles%2Caps%2C303&ref=sr_nr_p_89_16"
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

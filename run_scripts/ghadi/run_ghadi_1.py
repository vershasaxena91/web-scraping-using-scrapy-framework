import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Ghadi-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_ghadi",
        run_summary_file="run_summary_ghadi",
        company_client="Ghadi",
        bsr_100_links=[],
        search_list_links=[
            "https://tinyurl.com/4hzm9m9n",  # https://www.amazon.in/s?k=detergent&i=hpc&rh=n%3A1350384031%2Cp_89%3AAriel%7CGhadi%7CRIN%7CSurf+Excel%7CTide&dc&crid=3GVJXD5Z568P6&qid=1646200079&rnid=3837712031&sprefix=detergent+powder%2Caps%2C463&ref=sr_nr_p_89_5
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

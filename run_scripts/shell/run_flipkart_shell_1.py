import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Shell-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        rank_move=True,
        insert_at_rank=False,
        search_list=False,
        force_info_scrape=False,
        price_bsr_move=True,
        comments=False,
        sos=False,
        mongo_db="flipkart_marketplace_scraping_shell",
        run_summary_file="run_summary_flipkart_shell",
        company_client="Shell",
        rank_list_links=[
            "https://www.flipkart.com/automotive-accessories/oils-and-lubricants/pr?sid=1mt,0mi&marketplace=FLIPKART"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

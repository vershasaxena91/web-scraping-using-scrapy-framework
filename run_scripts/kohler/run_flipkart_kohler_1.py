import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Kohler-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_kohler",
        run_summary_file="run_summary_flipkart_kohler",
        company_client="Kohler",
        rank_list_links=[
            "https://www.flipkart.com/building-materials-and-supplies/bathroom-and-kitchen-fittings/pr?sid=b8s,ecr&marketplace=FLIPKART"
        ],
        search_list_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

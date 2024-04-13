import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Duracell-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_duracell",
        run_summary_file="run_summary_flipkart_duracell",
        company_client="Duracell",
        rank_list_links=[
            "https://www.flipkart.com/mobile-accessories/batteries/pr?sid=tyy,4mr,w65&marketplace=FLIPKART&otracker=product_breadCrumbs_Batteries"
        ],
        search_list_links=[
            "https://www.flipkart.com/mobile-accessories/batteries/pr?sid=tyy%2C4mr%2Cw65&marketplace=FLIPKART&otracker=product_breadCrumbs_Batteries&p%5B%5D=facets.brand%255B%255D%3DPanasonic&p%5B%5D=facets.brand%255B%255D%3DDURACELL&p%5B%5D=facets.brand%255B%255D%3DNippo&p%5B%5D=facets.brand%255B%255D%3DEVEREADY&p%5B%5D=facets.brand%255B%255D%3DEnergizer"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Ghadi-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_ghadi",
        run_summary_file="run_summary_flipkart_ghadi",
        company_client="Ghadi",
        rank_list_links=[
            "https://www.flipkart.com/home-cleaning-bathroom-accessories/household-supplies/washing-powders/pr?sid=rja,plv,bwz&otracker=categorytree"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=detergent+powders&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.brand%255B%255D%3DSurf%2Bexcel&p%5B%5D=facets.brand%255B%255D%3DAriel&p%5B%5D=facets.brand%255B%255D%3DTide&p%5B%5D=facets.brand%255B%255D%3DRin&p%5B%5D=facets.brand%255B%255D%3DGhadi"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

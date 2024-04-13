import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_havells",
        run_summary_file="run_summary_flipkart_havells",
        company_client="Havells",
        rank_list_links=[
            "https://www.flipkart.com/health-personal-care-appliances/personal-care-appliances/pr?sid=zlw%2C79s&marketplace=FLIPKART&sort=popularity"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=trimmers&otracker=search&otracker1=search&marketplace=FLIPKART"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

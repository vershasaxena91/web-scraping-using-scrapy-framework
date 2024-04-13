import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Lloyd_Washing_Machine-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        rank_move=False,
        insert_at_rank=False,
        search_list=False,
        force_info_scrape=False,
        price_bsr_move=False,
        comments=True,
        sos=False,
        mongo_db="flipkart_marketplace_scraping_lloyd_washing_machine",
        run_summary_file="run_summary_flipkart_lloyd_washing_machine",
        company_client="Lloyd_Washing_Machine",
        rank_list_links=[
            "https://www.flipkart.com/washing-machines/pr?sid=j9e,abm,8qx&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2F8qx&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DSAMSUNG",  # Samsung
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2F8qx&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DHaier",  # Haier
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2F8qx&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DBOSCH",  # Bosch
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2F8qx&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DWhirlpool",  # Whirlpool
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2F8qx&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DLloyd",  # Lloyd
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2F8qx&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DVoltas%2BBeko",  # Voltas Beko
        ],
        sos_keywords=keywords,
    )
    reactor.run()

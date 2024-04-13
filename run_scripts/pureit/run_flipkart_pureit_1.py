import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Pureit-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_pureit",
        run_summary_file="run_summary_flipkart_pureit",
        company_client="PureIt",
        rank_list_links=[
            "https://www.flipkart.com/water-purifiers/pr?sid=j9e,abm,i45&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=water+purifier&sid=j9e%2Cabm%2Ci45&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_na&otracker1=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_na&as-pos=1&as-type=RECENT&suggestionId=water+purifier%7CWater+purifiers&requestId=65c5eb44-ebec-4d64-aaee-1da30e1c1cf6&as-searchtext=water+puri&p%5B%5D=facets.brand%255B%255D%3DPureit&p%5B%5D=facets.brand%255B%255D%3DBlue%2BStar&p%5B%5D=facets.brand%255B%255D%3DKENT&p%5B%5D=facets.brand%255B%255D%3DEUREKA%2BFORBES&p%5B%5D=facets.brand%255B%255D%3DLIVPURE"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

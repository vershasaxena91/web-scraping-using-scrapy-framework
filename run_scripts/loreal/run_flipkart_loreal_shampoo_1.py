import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Loreal-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_loreal_shampoo",
        run_summary_file="run_summary_loreal_shampoo",
        company_client="Loreal",
        rank_list_links=[
            "https://www.flipkart.com/beauty-and-grooming/hair-care-and-accessory/hair-care/shampoo/pr?sid=g9b%2Clcf%2Cqqm%2Ct36&marketplace=FLIPKART"
        ],
        search_list_links=[
            # "https://www.flipkart.com/search?sid=g9b%2Clcf%2Cqqm%2Ct36&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DDOVE",  # Dove
            # "https://www.flipkart.com/search?sid=g9b%2Clcf%2Cqqm%2Ct36&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DTRESemme",  # Tresemme
            # "https://www.flipkart.com/search?sid=g9b%2Clcf%2Cqqm%2Ct36&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DBIOTIQUE",  # Biotique
            "https://www.flipkart.com/search?q=loreal+paris+shampoo+and+conditioner&sid=g9b%2Clcf%2Cqqm%2Ct36&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_2_12_na_na_na&otracker1=AS_QueryStore_OrganicAutoSuggest_2_12_na_na_na&as-pos=2&as-type=RECENT&suggestionId=loreal+paris+shampoo+and+conditioner%7CShampoo&requestId=9764ae8f-535a-4a7c-a0f3-a1d2dcf6388f&as-backfill=on",  # Loreal Paris
            # "https://www.flipkart.com/search?sid=g9b%2Clcf%2Cqqm%2Ct36&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DHEAD%2B%2526%2BSHOULDERS",  # Head & Shoulders
            "https://www.flipkart.com/beauty-and-grooming/hair-care-and-accessory/hair-care/shampoo/pr?sid=g9b,lcf,qqm,t36&q=loreal+professionnel&otracker=categorytree",  # Loreal Professionnel
        ],
        sos_keywords=keywords,
    )
    reactor.run()

import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Park_Avenue-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_park_avenue",
        run_summary_file="run_summary_flipkart_park_avenue",
        company_client="Park_Avenue",
        rank_list_links=[
            "https://www.flipkart.com/beauty-and-grooming/fragrances/deodorants/deodorant-spray/pr?sid=g9b,0yh,vp1,0kb&marketplace=FLIPKART&otracker=product_breadCrumbs_Deodorant+Spray"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=deodorant&sid=g9b%2C0yh%2Cvp1%2C0kb&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_5_na_na_na&otracker1=AS_QueryStore_OrganicAutoSuggest_1_5_na_na_na&as-pos=1&as-type=RECENT&suggestionId=deodorant%7CDeodorant+Spray&requestId=4fe097b7-116b-4167-8006-3922d119e0fb&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DFOGG&p%5B%5D=facets.brand%255B%255D%3DPARK%2BAVENUE&p%5B%5D=facets.brand%255B%255D%3DSET%2BWET&p%5B%5D=facets.brand%255B%255D%3DAXE&p%5B%5D=facets.brand%255B%255D%3DBRUT&p%5B%5D=facets.brand%255B%255D%3DYARDLEY&p%5B%5D=facets.brand%255B%255D%3DNIVEA"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

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
        rank_move=False,
        insert_at_rank=False,
        search_list=False,
        force_info_scrape=False,
        price_bsr_move=False,
        comments=False,
        sos=True,
        mongo_db="flipkart_marketplace_scraping_loreal_shampoo",
        run_summary_file="run_summary_loreal_shampoo",
        company_client="Loreal",
        rank_list_links=[
            "https://www.flipkart.com/beauty-and-grooming/hair-care-and-accessory/hair-care/shampoo/pr?sid=g9b%2Clcf%2Cqqm%2Ct36&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=shampoo&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.brand%255B%255D%3DL%2527Or%25C3%25A9al%2BParis",
            "https://www.flipkart.com/search?q=shampoo&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.brand%255B%255D%3DTRESemme",
            "https://www.flipkart.com/search?q=shampoo&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.brand%255B%255D%3DBIOTIQUE",
            "https://www.flipkart.com/search?q=shampoo&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.brand%255B%255D%3DHEAD%2B%2526%2BSHOULDERS",
            "https://www.flipkart.com/search?q=shampoo&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&p%5B%5D=facets.brand%255B%255D%3DDOVE",
            "https://www.flipkart.com/beauty-and-grooming/hair-care-and-accessory/hair-care/shampoo/pr?sid=g9b,lcf,qqm,t36&q=loreal+professionnel&otracker=categorytree",
        ],
        sos_keywords=keywords,
    )
    reactor.run()

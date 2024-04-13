import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_nykaa import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Loreal-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        brand_search=False,
        force_info_scrape=False,
        price_move=True,
        rank_move=True,
        comments=False,
        questions=False,
        sos=False,
        mongo_db="nykaa_marketplace_scraping_loreal_shampoo",
        run_summary_file="run_nykaa_summary_loreal",
        company_client="Loreal",
        brand_search_ids=[
            ["609", "316"],  # Dove # 43 products
            ["1251", "316"],  # L'Oreal Professionnel # 26 products
            ["595", "316"],  # L'Oreal Paris # 38 products
            ["923", "316"],  # Biotique # 39 products
            ["607", "316"],  # Tresemme # 32 products
            ["659", "316"],  # Head & Shoulders # 18 products
        ],
        rank_list_links=[
            "https://www.nykaa.com/hair-care/hair/shampoo/c/316?page_no=1"
        ],
        sos_keywords=keywords,
    )

    reactor.run()

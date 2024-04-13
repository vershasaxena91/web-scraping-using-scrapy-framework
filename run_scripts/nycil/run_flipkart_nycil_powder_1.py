import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nycil_Powder-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_nycil_powder",
        run_summary_file="run_summary_flipkart_nycil_powder",
        company_client="Nycil_Powder",
        rank_list_links=[
            "https://www.flipkart.com/beauty-and-grooming/body-face-skin-care/body-and-face-care/talcum-powder/pr?sid=g9b%2Cema%2C5la%2Ctyb&otracker=categorytree"
        ],
        search_list_links=[
            "https://www.flipkart.com/beauty-and-grooming/body-face-skin-care/body-and-face-care/talcum-powder/pr?sid=g9b%2Cema%2C5la%2Ctyb&otracker=categorytree&p%5B%5D=facets.brand%255B%255D%3DNYCIL",  # nycil # 39
            "https://www.flipkart.com/beauty-and-grooming/body-face-skin-care/body-and-face-care/talcum-powder/pr?sid=g9b%2Cema%2C5la%2Ctyb&otracker=categorytree&p%5B%5D=facets.brand%255B%255D%3DCandid",  # candid prickly # 1
            "https://www.flipkart.com/beauty-and-grooming/body-face-skin-care/body-and-face-care/talcum-powder/pr?sid=g9b%2Cema%2C5la%2Ctyb&otracker=categorytree&p%5B%5D=facets.brand%255B%255D%3DNavratna",  # navratna # 11
            "https://www.flipkart.com/beauty-and-grooming/body-face-skin-care/body-and-face-care/talcum-powder/pr?sid=g9b%2Cema%2C5la%2Ctyb&otracker=categorytree&p%5B%5D=facets.brand%255B%255D%3DDermiCool",  # dermi cool # 8
            "https://www.flipkart.com/beauty-and-grooming/body-face-skin-care/body-and-face-care/talcum-powder/pr?sid=g9b%2Cema%2C5la%2Ctyb&otracker=categorytree&p%5B%5D=facets.brand%255B%255D%3DSHOWER%2BTO%2BSHOWER",  # shower to shower # 23
        ],
        sos_keywords=keywords,
    )
    reactor.run()

import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Lloyd_Refrigerator-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_lloyd_refrigerator",
        run_summary_file="run_summary_flipkart_lloyd_refrigerator",
        company_client="Lloyd_Refrigerator",
        rank_list_links=[
            "https://www.flipkart.com/refrigerators/pr?sid=j9e,abm,hzg&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DHaier",  # haier
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DSAMSUNG",  # samsung
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DLG",  # lg
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DWhirlpool",  # whirlpool
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DLloyd",  # lloyd
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DLiebherr",  # liebherr
            "https://www.flipkart.com/search?sid=j9e%2Fabm%2Fhzg&otracker=CLP_Filters&p%5B%5D=facets.brand%255B%255D%3DVoltas",  # voltas
        ],
        sos_keywords=keywords,
    )
    reactor.run()

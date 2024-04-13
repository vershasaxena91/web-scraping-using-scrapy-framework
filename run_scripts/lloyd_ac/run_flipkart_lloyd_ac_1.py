import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Lloyd_AC-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_lloyd_ac",
        run_summary_file="run_summary_flipkart_lloyd_ac",
        company_client="Lloyd_AC",
        rank_list_links=[
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e,abm,c54&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e%2Cabm%2Cc54&otracker=categorytree&otracker=nmenu_sub_TVs+%26+Appliances_0_LG&p%5B%5D=facets.brand%255B%255D%3DLloyd",  # lloyds
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e%2Cabm%2Cc54&otracker=categorytree&otracker=nmenu_sub_TVs+%26+Appliances_0_LG&p%5B%5D=facets.brand%255B%255D%3DVoltas",  # voltas
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e%2Cabm%2Cc54&otracker=categorytree&otracker=nmenu_sub_TVs+%26+Appliances_0_LG&p%5B%5D=facets.brand%255B%255D%3DPanasonic",  # panasonic
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e%2Cabm%2Cc54&otracker=categorytree&otracker=nmenu_sub_TVs+%26+Appliances_0_LG&p%5B%5D=facets.brand%255B%255D%3DBlue%2BStar",  # bluestar
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e,abm,c54&p[]=facets.fulfilled_by%255B%255D%3DFlipkart%2BAssured&p[]=facets.brand%255B%255D%3DLG&p[]=facets.serviceability%5B%5D%3Dtrue&otracker=categorytree&otracker=nmenu_sub_TVs%20%26%20Appliances_0_LG",  # lg
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e%2Cabm%2Cc54&otracker=categorytree&otracker=nmenu_sub_TVs+%26+Appliances_0_LG&p%5B%5D=facets.brand%255B%255D%3DDaikin",  # daikin
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e%2Cabm%2Cc54&otracker=categorytree&otracker=nmenu_sub_TVs+%26+Appliances_0_LG&p%5B%5D=facets.brand%255B%255D%3DHitachi",  # hitachi
        ],
        sos_keywords=keywords,
    )
    reactor.run()

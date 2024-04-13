import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells_Light-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_havells_light",
        run_summary_file="run_summary_flipkart_havells_light",
        company_client="Havells_Light",
        rank_list_links=[
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&p&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&p%5B%5D=facets.brand%255B%255D%3DHAVELLS&otracker=categorytree&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting",  # Havells
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&otracker=categorytree&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&p%5B%5D=facets.brand%255B%255D%3DPHILIPS",  # Philips
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&otracker=categorytree&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&p%5B%5D=facets.brand%255B%255D%3DSyska%2BLed%2BLights&p%5B%5D=facets.brand%255B%255D%3DSyska",  # Syska
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&otracker=categorytree&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&p%5B%5D=facets.brand%255B%255D%3DCrompton&p%5B%5D=facets.brand%255B%255D%3DCrompton%2BGreaves",  # Crompton
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&otracker=categorytree&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&p%5B%5D=facets.brand%255B%255D%3DORIENT&p%5B%5D=facets.brand%255B%255D%3DOrient%2BElectric",  # Orient
            "https://www.flipkart.com/home-lighting/pr?sid=jhg&otracker=categorytree&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&otracker=nmenu_sub_Home+%26+Furniture_0_Home+Lighting&p%5B%5D=facets.brand%255B%255D%3DBAJAJ",  # Bajaj
        ],
        sos_keywords=keywords,
    )
    reactor.run()

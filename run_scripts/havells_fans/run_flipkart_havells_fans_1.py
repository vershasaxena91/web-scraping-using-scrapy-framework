import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells_Fans-Keywords.csv", header=None)
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
        mongo_db="flipkart_marketplace_scraping_havells_fans",
        run_summary_file="run_summary_flipkart_havells_fans",
        company_client="Havells_Fans",
        rank_list_links=[
            "https://www.flipkart.com/fans/pr?sid=j9e,abm,lbz&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=havells+fan&sid=j9e%2Cabm%2Clbz&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_11_na_na_na&otracker1=AS_QueryStore_OrganicAutoSuggest_1_11_na_na_na&as-pos=1&as-type=RECENT&suggestionId=havells+fan%7CFans&requestId=46a151c6-e3c3-4545-9949-c4146a0d2719&as-searchtext=havells+fan&p%5B%5D=facets.brand%255B%255D%3DHAVELLS",
            "https://www.flipkart.com/search?q=bajaj+fan&sid=j9e%2Cabm%2Clbz&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_9_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_9_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=bajaj+fan%7CFans&requestId=119ad80d-9974-4b70-8ccd-9fad337ff19d&as-searchtext=bajaj+fan&p%5B%5D=facets.brand%255B%255D%3DBAJAJ",
            "https://www.flipkart.com/search?q=orient+fan&sid=j9e%2Cabm%2Clbz&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=orient+fan%7CFans&requestId=a65bec94-2138-4bca-9da7-1696c27e9e13&as-searchtext=orient+fan&p%5B%5D=facets.brand%255B%255D%3DOrient%2BElectric",
            "https://www.flipkart.com/search?q=crompton+fan&sid=j9e%2Cabm%2Clbz&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_12_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_12_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=crompton+fan%7CFans&requestId=eb95c75b-834f-43a1-a57a-44ad13690c14&as-searchtext=crompton+fan&p%5B%5D=facets.brand%255B%255D%3DCROMPTON",
        ],
        sos_keywords=keywords,
    )
    reactor.run()

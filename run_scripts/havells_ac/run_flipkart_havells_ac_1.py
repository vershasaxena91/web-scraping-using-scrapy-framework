import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run_flipkart import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells_AC-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        rank_move=False,
        insert_at_rank=False,
        search_list=False,
        force_info_scrape=False,
        price_bsr_move=True,
        comments=False,
        sos=False,
        mongo_db="flipkart_marketplace_scraping_havells_ac",
        run_summary_file="run_summary_flipkart_havells_ac",
        company_client="Havells_AC",
        rank_list_links=[
            "https://www.flipkart.com/air-conditioners/pr?sid=j9e,abm,c54&marketplace=FLIPKART"
        ],
        search_list_links=[
            "https://www.flipkart.com/search?q=lloyd+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_5_na_na_na&otracker1=AS_QueryStore_OrganicAutoSuggest_1_5_na_na_na&as-pos=1&as-type=RECENT&suggestionId=lloyd+ac%7CAir+Conditioners&requestId=4f0c4942-422a-4c0d-a380-6a5524effead&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DLloyd",
            "https://www.flipkart.com/search?q=blue+star+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=blue+star+ac%7CAir+Conditioners&requestId=2a194495-38f4-4f08-b3ad-76ea5e6324c7&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DBlue%2BStar",
            "https://www.flipkart.com/search?q=panasonic+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_10_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=panasonic+ac%7CAir+Conditioners&requestId=663c3eb8-9d3a-4886-8b85-4faf810dec31&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DPanasonic",
            "https://www.flipkart.com/search?q=haier+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_5_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_5_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=haier+ac%7CAir+Conditioners&requestId=a4945867-6fdb-4a99-ad17-7a8c5defd33b&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DHaier",
            "https://www.flipkart.com/search?q=voltas+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_9_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_9_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=voltas+ac%7CAir+Conditioners&requestId=47582eb6-e479-4e48-9dfa-8f35d47c52c0&as-searchtext=voltas+ac&p%5B%5D=facets.brand%255B%255D%3DVoltas",
            "https://www.flipkart.com/search?q=daikin+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_6_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_6_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=daikin+ac%7CAir+Conditioners&requestId=e4d93b4d-e78b-4eda-b8fa-46883333cfc8&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DDaikin",
            "https://www.flipkart.com/search?q=lg+ac&sid=j9e%2Cabm%2Cc54&as=on&as-show=on&otracker=AS_QueryStore_OrganicAutoSuggest_1_3_na_na_ps&otracker1=AS_QueryStore_OrganicAutoSuggest_1_3_na_na_ps&as-pos=1&as-type=RECENT&suggestionId=lg+ac%7CAir+Conditioners&requestId=f5215375-ab58-4f32-8361-b170f933ca2f&as-backfill=on&p%5B%5D=facets.brand%255B%255D%3DLG",
        ],
        sos_keywords=keywords,
    )
    reactor.run()

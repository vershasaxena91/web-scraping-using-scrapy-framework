import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells_AC-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=True,
        total_questions_move=True,
        comments=False,
        questionAnswers=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_havells_ac",
        run_summary_file="run_summary_havells_ac",
        company_client="Havells_AC",
        bsr_100_links=[],
        search_list_links=[
            "https://tinyurl.com/4348mh4a",  # https://www.amazon.in/s?k=Lloyd&rh=n%3A3474656031%2Cp_89%3ALloyd&dc&qid=1645610750&rnid=3837712031&ref=sr_nr_p_89_1
            "https://tinyurl.com/yxymunc7",  # https://www.amazon.in/s?k=blue+star+ac&rh=n%3A3474656031%2Cp_89%3ABLUE+STAR&dc&qid=1646206729&rnid=3837712031&ref=sr_nr_p_89_1
            "https://tinyurl.com/mr49dmc3",  # https://www.amazon.in/s?k=Haier+ac&rh=n%3A3474656031%2Cp_89%3AHaier&dc&crid=AXNDD1CKZST2&qid=1645610995&rnid=3837712031&sprefix=haier+ac%2Caps%2C447&ref=sr_nr_p_89_1
            "https://tinyurl.com/2ebkf43e",  # https://www.amazon.in/s?k=Voltas+ac&i=kitchen&rh=n%3A3474656031%2Cp_89%3AVoltas&dc&crid=2ISW4ISD6DHIO&qid=1645611049&rnid=3837712031&sprefix=voltas+ac%2Caps%2C474&ref=sr_nr_p_89_1
            "https://tinyurl.com/2p8whrsc",  # https://www.amazon.in/s?k=Daikin+ac&rh=n%3A3474656031%2Cp_89%3ADaikin&dc&crid=2R2P71Y1UZCFI&qid=1646208122&rnid=3837712031&sprefix=daikin+ac%2Caps%2C242&ref=sr_nr_p_89_1
            "https://tinyurl.com/2j559as2",  # https://www.amazon.in/s?k=LG+ac&rh=n%3A3474656031%2Cp_89%3ALG&dc&qid=1646208177&rnid=3837712031&ref=sr_nr_p_89_1
        ],
        brand_search_links=[
            "https://www.amazon.in/stores/page/BDB74DA5-A67A-4B5B-9E38-FA6345BB38D4?ingress=0&visitId=852cd485-c430-4fbf-acab-10ea5e740ee8&lp_slot=auto-sparkle-hsa-tetris&store_ref=SB_A04958873RKTJ0W8RP0PO&ref_=sbx_be_s_sparkle_lsi4d_cta"
        ],
        sos_keywords=keywords,
    )
    reactor.run()

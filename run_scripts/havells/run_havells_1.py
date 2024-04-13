import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_havells",
        run_summary_file="run_summary_havells",
        company_client="Havells",
        brand_search_links=[
            "https://www.amazon.in/stores/page/6A91799A-9A7B-4B09-B4C3-04F971A24437",  # Havells All
            "https://www.amazon.in/stores/page/8397608F-5E88-4F70-9CFA-339803CACD83",  # Philips Male Grooming
            "https://www.amazon.in/stores/page/DA2E4275-41E2-4275-BBB1-D054DE69DF78",  # Vega Women Grooming
            "https://www.amazon.in/stores/page/338DAD93-A53F-4AA5-BB8F-A3F762C2C9AF",  # Vega Men Grooming
            "https://www.amazon.in/stores/page/A7DC0450-4EFF-4F45-804B-40EE2A3E3F01",  # Nova Men
            "https://www.amazon.in/stores/page/308CBE7F-1C68-4B0C-94D8-C12409274AD9",  # Nova Women
            "https://www.amazon.in/stores/page/213F9F5A-90C2-4B42-9A68-5AD8AAEACEF8",  # Philips Hair Straightner
            "https://www.amazon.in/stores/page/0262CCFE-40A8-470E-9627-D10EAC409351",  # Philips Hair Dryer
            "https://www.amazon.in/stores/page/A2CCB269-9313-4599-9FD2-A0709820E7B5",  # Philips Women Hair Removal
            "https://www.amazon.in/stores/page/91D0ECA3-FE41-47EA-B3D5-16A451335B7F",  # Syska Mens Grooming
            "https://www.amazon.in/stores/page/3208654E-3C36-49D1-9DCB-93A2AB6BDAAC",  # Syska Women Beauty Appliances
        ],
        sos_keywords=keywords,
    )
    reactor.run()

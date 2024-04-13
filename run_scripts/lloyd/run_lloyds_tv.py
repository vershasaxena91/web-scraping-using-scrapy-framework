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
        mongo_db="amazon_marketplace_scraping_havells_tv",
        run_summary_file="run_summary_havells_tv",
        company_client="Havells_TV",
        bsr_100_links=[
            "https://www.amazon.in/gp/bestsellers/electronics/1389396031/ref=zg_bs_nav_electronics_2_1389375031",
            "https://www.amazon.in/gp/bestsellers/electronics/1389396031/ref=zg_bs_pg_2?ie=UTF8&pg=2",
        ],
        search_list_links=["https://www.amazon.in/s?k=tv"],
        brand_search_links=[
            "https://www.amazon.in/stores/page/30D73111-6FA4-4AE7-9A79-0CF847465C76/search?ref_=ast_bln&terms=tv",  # Lloyd
            "https://www.amazon.in/stores/page/CA954A9D-23A3-4049-970E-7D0D7A2E647E",  # Samsung QLED 8K
            "https://www.amazon.in/stores/page/B48A3E27-BA44-40A1-912C-164B00E569EA",  # Samsung QLED 4K
            "https://www.amazon.in/stores/page/665E20E1-5839-4B4F-BDED-88DBBCFE85BA",  # Samsung Wondertainment
            "https://www.amazon.in/stores/page/9CB2DB65-EC84-43C1-AD2B-56D29487031F",  # Samsung Funbelievable
            "https://www.amazon.in/stores/page/B97D49FA-1968-4FDD-A8C0-6019D9EEE537",  # Samsung The Serif
            "https://www.amazon.in/stores/page/D6F97ECE-B95C-42F7-861F-4B3BF3DF2B5C",  # LG SIGNATURE AI ThinQ TV
            "https://www.amazon.in/stores/page/8902697C-6BBB-476B-BF56-FC7ACB76F4DF",  # LG OLED TV
            "https://www.amazon.in/stores/page/A3F39836-EA90-4AE1-9C93-D592087B4D7E",  # LG NanoCell AI ThinQ TV
            "https://www.amazon.in/stores/page/50E5C54D-7789-48D2-ADE9-09B5EC7EF4C8",  # LG UHD AI ThinQ TV
            "https://www.amazon.in/stores/page/101F1633-4585-4055-B899-73AD35043146",  # LG Smart AI ThinQ TV
            "https://www.amazon.in/stores/page/135360EB-98B1-4F52-8260-092B4E8F90BC",  # LG LED TV
            "https://www.amazon.in/stores/page/00E49594-BEBD-4C3F-98C5-411FFE8FA744",  # LG OLED Technology
            "https://www.amazon.in/stores/page/064CA889-A678-47EC-A40F-9DD1DD1DA471",  # Panasonic
            "https://www.amazon.in/stores/page/26529EE7-E836-4DA6-A361-5C3D44F0D490",  # Haier
            "https://www.amazon.in/stores/page/B4E52B67-CB12-4B35-94FA-473DC4FCB953",  # Philips
        ],
        sos_keywords=keywords,
    )
    reactor.run()

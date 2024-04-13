import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Kitkat-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_kitkat",
        run_summary_file="run_summary_kitkat",
        company_client="Kitkat",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=kitkat&i=grocery&rh=n%3A2454178031%2Cp_89%3AKit+Kat%7CKitkat%7CNestle&dc&qid=1649673692&rnid=3837712031&sprefix=kitka%2Caps%2C299&ref=sr_nr_p_89_4",
            "https://www.amazon.in/s?k=snickers&rh=n%3A2454178031%2Cp_89%3ASnickers&dc&crid=1GFHD0CU2ZGBZ&qid=1649673741&rnid=3837712031&sprefix=snickers%2Caps%2C325&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=cadbury+silk&rh=n%3A2454178031%2Cp_89%3ACadbury&dc&crid=2CTMLPHUK5WEQ&qid=1649673838&rnid=3837712031&sprefix=cadbury+silk%2Caps%2C294&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=crispello&rh=n%3A2454178031%2Cp_89%3ACadbury&dc&crid=1S3FAMVYUIUFD&qid=1649674286&rnid=3837712031&sprefix=crispello%2Caps%2C644&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=munch&rh=n%3A2454178031%2Cp_89%3ANestle&dc&pd_rd_r=aff1828e-705d-4b01-9000-a85165541f89&pd_rd_w=c6m3j&pd_rd_wg=DOlw1&pf_rd_p=206c771a-9255-43c3-91ee-be3d883c7b95&pf_rd_r=RF8VPBEN7G47532P7WWQ&qid=1649674335&rnid=3837712031&ref=sr_nr_p_89_1",
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

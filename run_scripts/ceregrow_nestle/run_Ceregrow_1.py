import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Ceregrow-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_ceregrow",
        run_summary_file="run_summary_ceregrow",
        company_client="Ceregrow",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=ceregrow&rh=n%3A2454178031%2Cp_89%3ANestl%C3%A9&dc&crid=21BJRXOB9MTY5&qid=1649676860&rnid=3837712031&sprefix=ceregro%2Caps%2C426&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=Kelloggs+Chocos&rh=n%3A2454178031%2Cp_89%3AKellogg%27s&dc&crid=2VWTKQ2M2SK47&qid=1649672522&rnid=3837712031&sprefix=kelloggs+chocos%2Caps%2C690&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=junior+horlicks&rh=n%3A1350384031%2Cp_89%3AHorlicks&dc&qid=1649672582&rnid=3837712031&sprefix=jun%2Caps%2C325&ref=sr_nr_p_89_1",
            "https://www.amazon.in/s?k=Pediasure&rh=n%3A1350384031%2Cp_89%3APediasure&dc&crid=Q7AGA0U0V4R8&qid=1649672616&rnid=3837712031&sprefix=pediasure%2Caps%2C334&ref=sr_nr_p_89_1",
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

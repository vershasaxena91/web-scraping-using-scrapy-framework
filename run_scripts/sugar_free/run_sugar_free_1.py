import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Sugar_Free-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_sugar_free",
        run_summary_file="run_summary_sugar_free",
        company_client="Sugar_Free",
        bsr_100_links=[],
        search_list_links=[],
        brand_search_links=[
            "https://www.amazon.in/stores/page/5AB0BF5C-A14F-44BA-9A62-9D2257D5BC21?ingress=2&visitId=05f0dfa5-eacc-4ec9-9b0a-bee109ad34fe&ref_=ast_bln",  # Equal
        ],
        sos_keywords=keywords,
    )
    reactor.run()

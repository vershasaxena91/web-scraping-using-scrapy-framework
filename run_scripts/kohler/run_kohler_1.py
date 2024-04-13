import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Kohler-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_kohler",
        run_summary_file="run_summary_kohler",
        company_client="Kohler",
        bsr_100_links=[],
        search_list_links=[
            "https://tinyurl.com/bddepmxy",  # https://www.amazon.in/s?i=home-improvement&bbn=9840692031&rh=n%3A9840692031%2Cp_89%3AALTON%7CAquieen%7CHindware%7CKohler%7CTOSCH&dc&qid=1646033589&rnid=3837712031&ref=sr_nr_p_89_3
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

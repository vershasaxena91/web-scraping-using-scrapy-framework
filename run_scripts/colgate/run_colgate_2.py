import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Colgate-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=False,
        total_questions_move=False,
        comments=True,
        questionAnswers=True,
        sos=False,
        mongo_db="amazon_marketplace_scraping_colgate",
        run_summary_file="run_summary_colgate",
        company_client="Colgate",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=colgate&i=hpc&rh=n%3A1374641031%2Cp_89%3AColgate&dc&ds=v1%3A2gIE3OEy1x89w1sXtz1TAgpKj%2FpHsprNa3e1O9oZQL4&crid=230ETII711SGQ&qid=1661141468&rnid=3837712031&sprefix=colgate+%2Caps%2C496&ref=sr_nr_p_89_1",  # Colgate
            "https://www.amazon.in/s?k=Pepsodent&i=hpc&rh=n%3A1374641031%2Cp_89%3APepsodent&dc&ds=v1%3AHhpbl61yCSXpi9qzHVAfE8ebLjG3mDnJimHWGLo43Jg&qid=1661141694&rnid=3837712031&ref=sr_nr_p_89_1",  # Pepsodent
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

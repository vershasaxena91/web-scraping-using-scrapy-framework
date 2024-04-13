import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Nestle-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_nestle",
        run_summary_file="run_summary_nestle",
        company_client="Nestle",
        bsr_100_links=[],
        search_list_links=[
            "https://tinyurl.com/4dpf6yn7",  # https://www.amazon.in/s?k=maggi&i=grocery&rh=n%3A2454178031%2Cp_89%3AMaggi&dc&crid=15ZZD0P846BCB&qid=1646399653&rnid=3837712031&sprefix=maggi%2Cgrocery%2C230&ref=sr_nr_p_89_1
            "https://tinyurl.com/33ym6yvs",  # https://www.amazon.in/s?k=kitkat&i=grocery&rh=n%3A2454178031%2Cp_89%3ANestle&dc&crid=2F4ICLMZ4TVA8&qid=1646399742&rnid=3837712031&sprefix=kitka%2Cgrocery%2C240&ref=sr_nr_p_89_2
            "https://tinyurl.com/2p8k3fpr",  # https://www.amazon.in/s?k=nestle+cerelac&i=hpc&rh=n%3A1350384031%2Cp_89%3ACERELAC&dc&qid=1646399888&rnid=3837712031&ref=sr_nr_p_89_1
            "https://tinyurl.com/2p92yahk",  # https://www.amazon.in/s?k=Nescafe+Gold+Cappuccino&i=grocery&rh=n%3A2454178031%2Cp_89%3ANescafe+Gold&dc&crid=6F1JBHRJ5CV0&qid=1646400380&rnid=3837712031&sprefix=nescafe+gold+%2Cgrocery%2C235&ref=sr_nr_p_89_2
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

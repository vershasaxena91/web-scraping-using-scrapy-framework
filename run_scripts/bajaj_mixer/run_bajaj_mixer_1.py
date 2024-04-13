import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Bajaj_Mixer-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_bajaj_mixer",
        run_summary_file="run_summary_bajaj_mixer",
        company_client="Bajaj_Mixer",
        bsr_100_links=[],
        search_list_links=[
            # "https://tinyurl.com/2p8xd42f",
            "https://www.amazon.in/s?k=mixer+grinder&i=kitchen&rh=n%3A976442031%2Cp_89%3ABajaj%7CButterfly%7CPHILIPS%7CPreethi%7CPrestige%7CWonderchef&dc&crid=1UKXKH7HSWEFY&qid=1648026664&rnid=3837712031&sprefix=mixer+grinder%2Ckitchen%2C210&ref=sr_nr_p_89_14"
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

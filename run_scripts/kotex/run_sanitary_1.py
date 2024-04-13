import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run

if __name__ == "__main__":
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        price_bsr_move=True,
        total_questions_move=True,
        comments=False,
        questionAnswers=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_sanitary_napkins",
        run_summary_file="run_summary_sanitary",
        company_client="GroupM Sanitary Napkins",
        search_list_links=[
            "https://tinyurl.com/4b74brcd"  # "https://www.amazon.in/sanitary-pads-for-women-Personal/s?k=sanitary+pads+for+women&rh=n%3A1374606031%2Cp_89%3AClovia%7CCojin%7CEvereve%7CKotex%7CWonderize"
        ],
        sos_keywords=[
            "Sanitary Napkins",
            "Sanitary Pads",
            "Sanitary Napkins Kotex",
            "Sanitary Napkins Cojin",
            "Sanitary Napkins Wondersize",
            "Sanitary Napkins Evereve",
            "Sanitary Napkins Clovia",
        ],
    )
    reactor.run()

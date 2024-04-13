import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Havells_Light-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_havells_light",
        run_summary_file="run_summary_havells_light",
        company_client="Havells_Light",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?i=kitchen&bbn=1380485031&rh=n%3A1380485031%2Cp_89%3AHavells&dc&qid=1656916276&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AXlOinbyLKbABR5WOMXGp8VJeuQgNTMlUObgPvxwxL%2FI",  # Havells
            "https://www.amazon.in/s?i=kitchen&bbn=1380485031&rh=n%3A1380485031%2Cp_89%3APHILIPS&dc&qid=1656916261&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AU5Ux5S0YVPlNtQaDU4Fd5bbnwoZ2V1Gh71DkDgwRw48",  # Philips
            "https://www.amazon.in/s?bbn=1380485031&rh=n%3A1380485031%2Cp_89%3ASYSKA&dc&qid=1656916241&rnid=3837712031&ref=lp_1380485031_nr_p_89_1",  # Syska
        ],
        brand_search_links=[
            "https://www.amazon.in/stores/page/ACDEF98C-74DE-44EA-A432-5CC0EBB0B642?ingress=2&visitId=419d5515-95e9-486b-bb34-5f4f294f662c&ref_=ast_bln",  # Crompton
            "https://www.amazon.in/stores/page/1C03DE39-2E3D-4F2F-A3F8-E62B1E17621B/search?ingress=2&visitId=09a7f370-358f-4bc9-afe4-7b7468951a80&ref_=ast_bln&terms=lighting",  # Orient Electric
            "https://www.amazon.in/stores/page/0F14D851-B8A4-470B-A842-BD3BDE30E4E7/search?ref_=ast_bln&terms=Bajaj%20lighting",  # Bajaj
        ],
        sos_keywords=keywords,
    )
    reactor.run()

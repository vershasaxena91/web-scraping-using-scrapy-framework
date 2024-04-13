import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Safed_Detergent_Powder-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_safed_detergent_powder",
        run_summary_file="run_summary_safed_detergent_powder",
        company_client="Safed_Detergent_Powder",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?bbn=1374569031&rh=n%3A1374569031%2Cp_89%3ASurf+Excel&dc&qid=1667998241&rnid=3837712031&ref=lp_1374569031_nr_p_89_0",  # Surf Excel # 48
            "https://www.amazon.in/s?i=hpc&bbn=1374569031&rh=n%3A1374569031%2Cp_89%3ATide&dc&qid=1667998668&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3ABAMkzR%2FW9cHK9pKOptjbcTplceUTdVLVIvSghFlZ9Yw",  # Tide # 23
            "https://www.amazon.in/s?i=hpc&bbn=1374569031&rh=n%3A1374569031%2Cp_89%3ARIN&dc&qid=1667998948&rnid=3837712031&ref=sr_nr_p_89_1&ds=v1%3AZaNDY17FtYZlrslcsUggt8zTnmeg6oCx11wPLuQREjw",  # RIN # 8
            "https://www.amazon.in/s?i=hpc&bbn=1374569031&rh=n%3A1374569031%2Cp_89%3AAriel&dc&qid=1667999005&rnid=3837712031&ref=sr_nr_p_89_2&ds=v1%3AO2%2BHLTGCL7IzkSfh4eDPj80JtSbBqwZuyu50P03xQek",  # Ariel # 39
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

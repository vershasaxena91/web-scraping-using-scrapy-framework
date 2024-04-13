import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Loreal-Keywords.csv", header=None)
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
        mongo_db="amazon_marketplace_scraping_loreal",
        run_summary_file="run_summary_loreal",
        company_client="Loreal",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?k=loreal+professionnel&i=beauty&rh=n%3A1374334031%2Cp_89%3AL%27Or%C3%A9al+Professionnel&dc&ds=v1%3ACBbLrOdyo5O090t4sKxOoO4%2BzX26J5I1MzGn3SQtCk0&qid=1661327086&rnid=3837712031&ref=sr_nr_p_89_2",  # Loreal Professionnel
            "https://www.amazon.in/s?k=loreal+paris&i=beauty&rh=n%3A1374334031%2Cp_89%3AL%27Oreal+Paris&dc&ds=v1%3Ac3kZdYIYQiqB9JbwyMGzfDkVkeGG9zfFBSm1nnz603Y&qid=1661327116&rnid=3837712031&ref=sr_nr_p_89_1",  # Loreal Paris
            "https://www.amazon.in/s?k=dove&i=beauty&rh=n%3A1374334031%2Cp_89%3ADove&dc&ds=v1%3A%2FMA3MU1hgrbmYhLB5TX2k%2Bubm6sa%2BC6Fbs6Or4NcywQ&qid=1661327140&rnid=3837712031&ref=sr_nr_p_89_1",  # Dove
            "https://www.amazon.in/s?k=biotique&i=beauty&rh=n%3A1374334031%2Cp_89%3ABiotique&dc&ds=v1%3AjCiYhOh4%2F7C5OCX2m%2F8tRio5SdbNBbmzPOu%2FOPpbfKM&qid=1661327241&rnid=3837712031&ref=sr_nr_p_89_1",  # Biotique
            "https://www.amazon.in/s?k=head+%26+shoulders&i=beauty&rh=n%3A1374334031%2Cp_89%3AHead+%26+Shoulders&dc&ds=v1%3A2D4xMtGHGNxQA2MNXX3o%2BpvAjr8V0p2urk04kMa9wtw&qid=1661327292&rnid=3837712031&ref=sr_nr_p_89_1",  # Head & Shoulders
            "https://www.amazon.in/s?k=tresemme&i=beauty&rh=n%3A1374334031%2Cp_89%3ATRESemme&dc&ds=v1%3AP1NlyPk%2BXHsgYgzNTGinkMlKqaHZqdbS1rOy7CrRPWU&qid=1661327324&rnid=3837712031&ref=sr_nr_p_89_1",  # Tresemme
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

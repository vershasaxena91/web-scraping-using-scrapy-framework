import sys

# setting path
sys.path.append("../../../web-scraping")
sys.path.append(".")

from twisted.internet import reactor
from run import run
import pandas as pd

if __name__ == "__main__":
    keywords = pd.read_csv("Biba-Keywords.csv", header=None)
    keywords = keywords[0].to_list()
    print(keywords)
    run(
        bsr_100=False,
        search_list=False,
        brand_search=False,
        force_info_scrape=False,
        price_bsr_move=True,
        total_questions_move=False,
        comments=False,
        questionAnswers=False,
        sos=False,
        mongo_db="amazon_marketplace_scraping_biba",
        run_summary_file="run_summary_biba",
        company_client="Biba",
        bsr_100_links=[],
        search_list_links=[
            "https://www.amazon.in/s?i=apparel&bbn=1968253031&rh=n%3A1571271031%2Cn%3A1953602031%2Cn%3A1968253031%2Cn%3A1968255031%2Cp_n_feature_nineteen_browse-bin%3A11301357031%2Cp_89%3ABIBA%2Cp_n_pct-off-with-tax%3A40-%2Cp_85%3A10440599031%2Cp_72%3A1318476031&s=apparels&dc&ds=v1%3AUcaUH4ZFjKmosNNSTrIhoh2dPEZacYfCrFteurNGGAA&pf_rd_i=1968255031&pf_rd_i=27063449031%2C27063449031%2C27063449031&pf_rd_m=A1VBAL9TL5WCBF&pf_rd_m=A1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF&pf_rd_p=05ee7b3c-028c-40e3-9734-8c20044f5cd5%2C0ed8c482-45e2-4e57-bd8e-13496fc2b5e4%2Cbf71831a-49b4-4693-9b1f-889882080807&pf_rd_p=121ed83f-8546-450e-94a8-61a8bf1257fc&pf_rd_r=0JDEP1CFFE0YXXTF37YV%2C6ZA3HSW0WAB0JN15PMN6%2CXCMWYBNBM7NB7SK9Q9W7&pf_rd_r=SRHXCJA4Z0YJH8TRYZRE&pf_rd_s=merchandised-search-10%2Cmerchandised-search-9%2Cmerchandised-search-9&pf_rd_s=merchandised-search-4&pf_rd_t=30901&qid=1675084596&rnid=1968253031&ref=sr_nr_n_1",  # Biba for Women's Kurtas & Kurtis # 48
            "https://www.amazon.in/s?i=apparel&bbn=1968255031&rh=n%3A1571271031%2Cn%3A1953602031%2Cn%3A1968253031%2Cn%3A1968255031%2Cp_n_feature_nineteen_browse-bin%3A11301357031%2Cp_n_pct-off-with-tax%3A40-%2Cp_85%3A10440599031%2Cp_72%3A1318476031%2Cp_89%3AW+for+Woman&s=apparels&dc&ds=v1%3AAkACheo6eE8Md0mdjaFH36sWw0oOHyW6pjSKUTvLLjU&pf_rd_i=1968255031&pf_rd_i=27063449031%2C27063449031%2C27063449031&pf_rd_m=A1VBAL9TL5WCBF&pf_rd_m=A1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF&pf_rd_p=05ee7b3c-028c-40e3-9734-8c20044f5cd5%2C0ed8c482-45e2-4e57-bd8e-13496fc2b5e4%2Cbf71831a-49b4-4693-9b1f-889882080807&pf_rd_p=121ed83f-8546-450e-94a8-61a8bf1257fc&pf_rd_r=0JDEP1CFFE0YXXTF37YV%2C6ZA3HSW0WAB0JN15PMN6%2CXCMWYBNBM7NB7SK9Q9W7&pf_rd_r=SRHXCJA4Z0YJH8TRYZRE&pf_rd_s=merchandised-search-10%2Cmerchandised-search-9%2Cmerchandised-search-9&pf_rd_s=merchandised-search-4&pf_rd_t=30901&qid=1675085157&rnid=3837712031&ref=sr_nr_p_89_2",  # W for Women's Kurtas & Kurtis # 48
            "https://www.amazon.in/s?i=apparel&bbn=1968253031&rh=n%3A1571271031%2Cn%3A1953602031%2Cn%3A1968253031%2Cn%3A3723380031%2Cp_n_feature_nineteen_browse-bin%3A11301357031%2Cp_89%3ABIBA%2Cp_n_pct-off-with-tax%3A40-%2Cp_85%3A10440599031%2Cp_72%3A1318476031&s=apparels&dc&ds=v1%3A8FSh6P%2FTErAhzClabp%2FWg6iioLCaF4RBEtZ3oNw0G4s&pf_rd_i=1968255031&pf_rd_i=27063449031%2C27063449031%2C27063449031&pf_rd_m=A1VBAL9TL5WCBF&pf_rd_m=A1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF&pf_rd_p=05ee7b3c-028c-40e3-9734-8c20044f5cd5%2C0ed8c482-45e2-4e57-bd8e-13496fc2b5e4%2Cbf71831a-49b4-4693-9b1f-889882080807&pf_rd_p=121ed83f-8546-450e-94a8-61a8bf1257fc&pf_rd_r=0JDEP1CFFE0YXXTF37YV%2C6ZA3HSW0WAB0JN15PMN6%2CXCMWYBNBM7NB7SK9Q9W7&pf_rd_r=SRHXCJA4Z0YJH8TRYZRE&pf_rd_s=merchandised-search-10%2Cmerchandised-search-9%2Cmerchandised-search-9&pf_rd_s=merchandised-search-4&pf_rd_t=30901&qid=1675137849&rnid=1968253031&ref=sr_nr_n_3",  # Biba for Women's Salwar Suits # 46
            "https://www.amazon.in/s?i=apparel&bbn=3723380031&rh=n%3A1571271031%2Cn%3A1953602031%2Cn%3A1968253031%2Cn%3A3723380031%2Cp_n_feature_nineteen_browse-bin%3A11301357031%2Cp_n_pct-off-with-tax%3A40-%2Cp_85%3A10440599031%2Cp_72%3A1318476031%2Cp_89%3AW+for+Woman&s=apparels&dc&ds=v1%3AD85p06SM15CRI%2FGJOOBtbTbdfFOZsWwfcsoORcenkSQ&pf_rd_i=1968255031&pf_rd_i=27063449031%2C27063449031%2C27063449031&pf_rd_m=A1VBAL9TL5WCBF&pf_rd_m=A1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF%2CA1VBAL9TL5WCBF&pf_rd_p=05ee7b3c-028c-40e3-9734-8c20044f5cd5%2C0ed8c482-45e2-4e57-bd8e-13496fc2b5e4%2Cbf71831a-49b4-4693-9b1f-889882080807&pf_rd_p=121ed83f-8546-450e-94a8-61a8bf1257fc&pf_rd_r=0JDEP1CFFE0YXXTF37YV%2C6ZA3HSW0WAB0JN15PMN6%2CXCMWYBNBM7NB7SK9Q9W7&pf_rd_r=SRHXCJA4Z0YJH8TRYZRE&pf_rd_s=merchandised-search-10%2Cmerchandised-search-9%2Cmerchandised-search-9&pf_rd_s=merchandised-search-4&pf_rd_t=30901&qid=1675137890&rnid=3837712031&ref=sr_nr_p_89_1",  # W for Women's Salwar Suits
            "https://www.amazon.in/s?k=fabindia&i=apparel&rh=n%3A1571271031%2Cn%3A1953602031%2Cn%3A1968253031%2Cn%3A1968255031%2Cp_89%3AFabindia&dc&ds=v1%3AfFfJ4QAug7ZvaabHNfuP%2FEuTFDDTg8MyTb1itXzYITY&qid=1675137563&rnid=1571271031&ref=sr_nr_n_6",  # Fabindia for Women's Kurtas & Kurtis # 114
            "https://www.amazon.in/s?k=fabindia&i=apparel&rh=n%3A1571271031%2Cn%3A1953602031%2Cn%3A1968253031%2Cn%3A3723380031%2Cp_89%3AFabindia&dc&ds=v1%3A%2FCfUwlTpdqAUPdolkrYDCo4ixlgIaAhPeZSsX1knwsg&qid=1675147248&rnid=1571271031&ref=sr_nr_n_8",  # Fabindia for Women's Salwar Suits # 8
        ],
        brand_search_links=[],
        sos_keywords=keywords,
    )
    reactor.run()

import pandas as pd

#


# data1 = pd.DataFrame(pd.read_csv('../kitkat/amazon_prods_with_ABSA.csv'))
data2 = pd.DataFrame(
    pd.read_csv(
        "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/flipkart_prods_with_ABSA.csv"
    )
)
# data3 = pd.DataFrame(pd.read_csv('../../../data_csvs/ceregrow/shopee_prods_with_ABSA.csv'))

# keywords = data1['keyword']
keywords2 = data2["keyword"]
# keywords = data3['keyword']

keywords = keywords2.append(keywords2, ignore_index=True)
# keywords = keywords.append(keywords3, ignore_index=True)
keywords = keywords.apply(str.lower)

keywords = keywords.drop_duplicates()
keywords.to_csv(
    "../../data/OutputData/nycil_powder/flipkart_marketplace_scraping/keywords.csv",
    index=False,
)

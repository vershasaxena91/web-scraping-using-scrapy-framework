import ast
import csv
import pandas as pd

# File path
file_path = "pureit_sos_data_updated.json"


def Convert_Json_To_CSV(file_path):
    # Read json file
    with open(file_path, encoding="utf8") as file:
        data = file.read()

    # Split data from "\n"
    data_split = list(data.split("\n"))

    # Convert data from string to list
    data_list = [ast.literal_eval("{%s}" % item[1:-1]) for item in data_split]
    # data_list = [ast.literal_eval(item[1:-1]) for item in data_split]

    # Save data in csv file
    keys = data_list[0].keys()
    with open(
        "amazon_product_scraping/data/OutputData/amazon_pureit_sos_data_updated.csv",
        "w",
        encoding="utf-8",
        newline="",
    ) as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data_list)
    df = pd.read_csv(
        "amazon_product_scraping/data/OutputData/amazon_pureit_sos_data_updated.csv"
    )
    df.drop(df.tail(1).index, inplace=True)
    df.to_csv(
        "amazon_product_scraping/data/OutputData/amazon_pureit_sos_data_updated.csv",
        index=False,
    )


# Call the function
Convert_Json_To_CSV(file_path)

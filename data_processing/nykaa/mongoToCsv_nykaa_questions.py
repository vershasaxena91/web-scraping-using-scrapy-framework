import pandas as pd
import json
from tqdm import tqdm
from datetime import datetime


class Converter_questions:
    def __init__(self, filename):

        self.data = [json.loads(line) for line in open(filename, encoding="utf8")]
        self.QAs = pd.DataFrame(columns=["ASIN", "Question", "Answer", "Votes", "Date"])

    def convert_doc(self, i):
        done = set()
        QAs = []
        for qa in self.data[i]["QAs"]:
            if str(qa) in done:
                continue
            done.add(str(qa))
            QAs.append(
                {
                    "ASIN": self.data[i]["product_id"],
                    "Question": qa["question"],
                    "Answer": qa["answer"],
                    "Votes": qa["votes"],
                    "Date": datetime.strptime(qa["date"], "%Y-%m-%d %H:%M:%S").strftime(
                        "%d-%m-%Y"
                    ),
                }
            )
        return QAs

    def convert(self):
        for i in tqdm(range(len(self.data))):
            QAs = self.convert_doc(i)
            self.QAs = self.QAs.append(QAs, ignore_index=True)

    def save(self, details_file):
        self.QAs.to_csv(details_file, index=False)


if __name__ == "__main__":

    cvt = Converter_questions(
        "../../data/InputData/loreal_shampoo/nykaa_marketplace_scraping/product_QAs.json"
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/loreal_shampoo/nykaa_marketplace_scraping/nykaa_QAs.csv"
    )

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
from tqdm import tqdm


class Converter_sos:
    def __init__(self, list_file, data_file) -> None:
        # with open(list_file, encoding='utf-8') as f:
        #     self.list = json.loads(f.read())['product_list']
        # with open(data_file, encoding='utf-8') as f:
        #     self.data = json.loads(f.read())['product_data']
        self.list = [json.loads(line) for line in open(list_file, encoding="utf8")]
        self.data = [json.loads(line) for line in open(data_file, encoding="utf8")]

        self.sos_df = pd.DataFrame(
            columns=[
                "Keyword",
                "Rank",
                "ASIN",
                "Title",
                "Brand",
                "Latest Selling Price",
                "Original Price",
                "Fulfilled",
            ]
        )
        self.reverse_asin = {}
        for prod in self.data:
            self.reverse_asin[prod["product_asin"]] = prod
        #     # if prod['product_brand'] is None or prod['product_brand'] == "":
        #     #     if "Park Avenue" in prod['product_name']:
        #     #         self.reverse_asin[prod['product_asin']]['product_brand'] = "Park Avenue"
        #     #     elif "Set Wet" in prod['product_name']:
        #     #         self.reverse_asin[prod['product_asin']]['product_brand'] = "Set Wet"
        #     #     else:
        #     #         self.reverse_asin[prod['product_asin']]['product_brand'] = prod['product_name'].split()[0]

        print(len(self.reverse_asin))
        asins = set()
        for d in self.list:
            for p in d["product_order"]:
                asins.add(p["product_asin"])
        print(len(asins))

    def convert_doc(self, doc):

        doc["product_order"] = sorted(
            doc["product_order"], key=lambda x: x["product_rank"]
        )

        sos_rank_list = []
        print(doc["keyword"], len(doc["product_order"]))
        i = 0
        present = set()

        for prod in doc["product_order"]:
            if self.reverse_asin[prod["product_asin"]]["product_brand"] in ["", "NA"]:
                if "Manyavar" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Manyavar"
                elif "VASTRAMAY" in self.reverse_asin[prod["product_asin"]]["product_name"] or "Vastramay" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "VASTRAMAY"
                elif "Ethnix by Raymond" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Ethnix by Raymond"
                elif "Excent" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Excent"
                elif "Uri and MacKenzie" in self.reverse_asin[prod["product_asin"]]["product_name"] or "UnM" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Uri and MacKenzie"
                elif "ENCINO" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ENCINO"
                elif "TheEthnicCo" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "TheEthnicCo"
                elif "Ethluxis" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Ethluxis"
                elif "XEPON" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "XEPON"
                elif "DUPATTA BAZAAR" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "DUPATTA BAZAAR"
                elif "Royal" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Royal"
                elif "MAHEK APPARELSS" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "MAHEK APPARELSS"
                elif "See Designs" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "See Designs"
                elif "Mag" in self.reverse_asin[prod["product_asin"]]["product_name"] or "MAG" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Mag"
                elif "Amazon Brand - Symbol" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Amazon Brand - Symbol"
                elif "Vida Loca" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Vida Loca"
                elif "Sanwara" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Sanwara"
                elif "Bewakoof" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Bewakoof"
                elif "Luxrio" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Luxrio"
                elif "Gauri Laxmi Enterprise" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Gauri Laxmi Enterprise"
                elif "Rajubhai Hargovindas" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Rajubhai Hargovindas"
                elif "Manthan" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Manthan"
                elif "Enmozz" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Enmozz"
                elif "Bewakoof" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Bewakoof"
                elif "U-TURN" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "U-TURN"
                elif "GauriLaxmi Enterprise" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "GauriLaxmi Enterprise"
                elif "Jompers" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Jompers"
                elif "ABH LIFESTYLE" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ABH LIFESTYLE"
                elif "ashtang" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ashtang"
                elif "Majestic Man" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Majestic Man"
                elif "FINIVO FASHION" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "FINIVO FASHION"
                elif "Enchanted Drapes" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Enchanted Drapes"
                elif "rytras" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "rytras"
                elif "Amayra" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Amayra"
                elif "Miraan" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Miraan"
                elif "VROJASS" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "VROJASS"
                elif "LookMark" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "LookMark"
                elif "AJ DEZINES" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "AJ DEZINES"
                elif "SG YUVRAJ" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "SG YUVRAJ"
                elif "AHHAAAA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "AHHAAAA"
                elif "SG RAJASAHAB" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "SG RAJASAHAB"
                elif "N.B.F Fashion" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "N.B.F Fashion"
                elif "laxmi Arts" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "laxmi Arts"
                elif "Zolario" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Zolario"
                elif "ANARVA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ANARVA"
                elif "CLASSY FASHION" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "CLASSY FASHION"
                elif "Brand Boy" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Brand Boy"
                elif "Modern Garments" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Modern Garments"
                elif "Krypmax" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Krypmax"
                elif "ONNIX" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ONNIX"
                elif "DIAMOND" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "DIAMOND STYLE EMBROIDERY"
                elif "BENSTOKE" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "BENSTOKE"
                elif "Amzira" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Amzira"
                elif "hangup" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "hangup"
                elif "Avaeta" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Avaeta"
                elif "SG LEMAN" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "SG LEMAN"
                elif "DREAM BLUE" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "DREAM BLUE"
                elif "Logass" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Logass"
                elif "Sadree" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Sadree"
                elif "HEY CROSS" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "HEY CROSS"
                elif "HEORA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "HEORA"
                elif "Panjatan" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Panjatan"
                elif "Pro-Ethic Style Developer" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Pro-Ethic Style Developer"
                elif "BLYX" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "BLYX"
                elif "SKAVIJ" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "SKAVIJ"
                elif "SGDONOR" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "SGDONOR"
                elif "Shri Goenka Sales" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Shri Goenka Sales"
                elif "Sunrise Traders" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Sunrise Traders"
                elif "Zombom" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Zombom"
                elif "Vida Loca" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Vida Loca"
                elif "FAB KALAKRITI" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "FAB KALAKRITI"
                elif "BENSTITCH" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "BENSTITCH"
                elif "Fashtastic" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Fashtastic"
                elif "ZAKOD" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ZAKOD"
                elif "ANREX" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "ANREX"
                elif "FILOSE" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "FILOSE"
                elif "SOJANYA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "SOJANYA"
                elif "LatestPlus Apparel" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "LatestPlus Apparel"
                elif "KRAFT INDIA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "KRAFT INDIA"
                elif "TULSSIKAA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "TULSSIKAA"
                elif "Veera Paridhaan" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Veera Paridhaan"
                elif "9 X" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "9 X"
                elif "Larwa" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Larwa"
                elif "Latest Chikan" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Latest Chikan"
                elif "Ethnic Factory" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Ethnic Factory"
                elif "BE ACTIVE" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "BE ACTIVE"
                elif "PYRAHAN" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "PYRAHAN"
                elif "Aks Creations" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Aks Creations"
                elif "Maharaja" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Maharaja"
                elif "Benzene Bite" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Benzene Bite"
                elif "NTIFIC" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "NTIFIC"
                elif "CROWN WORLD" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "CROWN WORLD"
                elif "CRYSTAL REVENUE" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "CRYSTAL REVENUE"
                elif "HARPITA" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "HARPITA"
                elif "RG DESIGNERS" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "RG DESIGNERS"
                elif "Mentific" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Mentific"
                elif "MANQ" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "MANQ"
                elif "Ikhodal Fashion" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Ikhodal Fashion"
                elif "Shiwam Ethnix" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Shiwam Ethnix"
                elif "A.K Enterprise" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "A.K Enterprise"
                elif "trustous" in self.reverse_asin[prod["product_asin"]]["product_name"] or "TRUSTOUS" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "trustous"
                elif "Varmohey" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Varmohey"
                elif "Trendshon" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Trendshon"
                elif "Sultan" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Sultan"
                elif "Fshway" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Fshway"
                elif "AVANEESH Enterprise" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "AVANEESH Enterprise"
                elif "FABWAX " in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "FABWAX "
                elif "Shubh Mangalam" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Shubh Mangalam"
                elif "CHAMUNDA MAA CREATION" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "CHAMUNDA MAA CREATION"
                elif "AishwarryaLaxmi" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "AishwarryaLaxmi"
                elif "koshin" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "koshin"
                elif "VMart" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "V Mart"
                elif "BRIGHT COLLECTION" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "BRIGHT COLLECTION"
                elif "Bhawna" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Bhawna"
                elif "Arshia Fashions" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Arshia Fashions"
                elif "Peter England" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Peter England"
                elif "BANHUSSAIN" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "BANHUSSAIN"
                elif "Lookslady Designer" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Lookslady Designer"
                elif "Divyadham Textiles" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Divyadham Textiles"
                elif "Saifoo" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Saifoo"
                elif "LADKU" in self.reverse_asin[prod["product_asin"]]["product_name"]:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "LADKU"
                else:
                    self.reverse_asin[prod["product_asin"]]["product_brand"] = "Generic"
            # print('########')
            # print(prod["product_asin"])
            # print((self.reverse_asin[prod["product_asin"]]["product_name"]).encode("utf-8"))
            # print('########')
            try:
                i += 1
                present.add(prod["product_asin"])
                sos_rank_list.append(
                    {
                        "Keyword": doc["keyword"],
                        "Rank": i,  # prod['product_rank'],
                        "ASIN": prod["product_asin"],
                        "Title": self.reverse_asin[prod["product_asin"]][
                            "product_name"
                        ],
                        "Brand": " ".join(
                            self.reverse_asin[prod["product_asin"]]["product_brand"]
                            .lower()
                            .split()
                        ).title(),
                        "Latest Selling Price": self.reverse_asin[prod["product_asin"]][
                            "product_sale_price"
                        ],
                        "Original Price": self.reverse_asin[prod["product_asin"]][
                            "product_original_price"
                        ],
                        "Fulfilled": self.reverse_asin[prod["product_asin"]][
                            "product_fullfilled"
                        ],
                        "Sponsored": prod["sponsored"],
                        "Present": True,
                    }
                )
            except:
                print(prod["product_asin"])

        for asin, prod in self.reverse_asin.items():
            if asin not in present:
                sos_rank_list.append(
                    {
                        "Keyword": doc["keyword"],
                        "Rank": -1,  # prod['product_rank'],
                        "ASIN": asin,
                        "Title": prod["product_name"],
                        "Brand": " ".join(
                            prod["product_brand"].lower().split()
                        ).title(),
                        "Latest Selling Price": prod["product_sale_price"],
                        "Original Price": prod["product_original_price"],
                        "Fulfilled": prod["product_fullfilled"],
                        # 'Sponsored': prod['sponsored'],
                        "Present": False,
                    }
                )
        return sos_rank_list

    def convert(self):
        def convert_string_to_day(sday):
            return datetime.strptime(sday, "%Y-%m-%d %H:%M:%S")
            # .strftime("%d-%m-%Y")

        # self.list = sorted(self.list, key=lambda x: x['time'])
        data_old = []
        data_last = []
        seen = set()
        self.list = sorted(
            self.list, key=lambda x: convert_string_to_day(x["time"]), reverse=True
        )
        for doc in self.list:
            if doc["keyword"] in seen:
                # if (datetime.today() - datetime.strptime(doc['time'], "%Y-%m-%d %H:%M:%S")).days > 1:
                data_old.append(doc)
            # if convert_string_to_day(doc['time']) == (datetime.today() - timedelta(days=0)).strftime("%d-%m-%Y"):
            else:
                data_last.append(doc)

        print(len(data_old), len(data_last))
        # data_old = sorted(data_old, key=lambda x: x['time'])
        for i in tqdm(range(len(data_old))):
            # print(data_old[i]['time'])
            sos_keyword = self.convert_doc(data_old[i])

        data_last = sorted(data_last, key=lambda x: x["time"], reverse=True)
        keywords = set()
        for i in tqdm(range(len(data_last))):
            # print(data_last[i]['time'])
            sos_keyword = self.convert_doc(data_last[i])
            if data_last[i]["keyword"] not in keywords:
                self.sos_df = self.sos_df.append(sos_keyword, ignore_index=True)
            keywords.add(data_last[i]["keyword"])

    def save(self, sos_file):
        self.sos_df.to_csv(sos_file, index=False)


if __name__ == "__main__":
    # cvt = Converter_sos('amazon_marketplace_scraping_havells_19_01_22/share_of_search.json', 'amazon_marketplace_scraping_havells_19_01_22/sos_data.json')
    # cvt.convert()
    # cvt.save('./amazon_sos.csv')

    cvt = Converter_sos(
        "../../data/InputData/manyavar/amazon_marketplace_scraping/share_of_search.json",
        "../../data/InputData/manyavar/amazon_marketplace_scraping/sos_data.json",
    )
    cvt.convert()
    cvt.save(
        "../../data/OutputData/manyavar/amazon_marketplace_scraping/amazon_sos.csv"
    )

    # cvt = Converter_sos('../shell/amazon_marketplace_scraping_shell_25_02_22/share_of_search.json', '../shell/amazon_marketplace_scraping_shell_25_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../shell/amazon_sos.csv')

    # cvt = Converter_sos('../duracell/amazon_marketplace_scraping_duracell_16_02_22/share_of_search.json', '../duracell/amazon_marketplace_scraping_duracell_16_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../duracell/amazon_sos.csv')

    # cvt = Converter_sos('../hafele/amazon_marketplace_scraping_hafele_21_02_22/share_of_search.json', '../hafele/amazon_marketplace_scraping_hafele_21_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../hafele/amazon_sos.csv')

    # cvt = Converter_sos('../ferrero/amazon_marketplace_scraping_ferrero_28_02_22/share_of_search.json', '../ferrero/amazon_marketplace_scraping_ferrero_28_02_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ferrero/amazon_sos.csv')

    # cvt = Converter_sos('../kohler/amazon_marketplace_scraping_kohler_03_03_22/share_of_search.json', '../kohler/amazon_marketplace_scraping_kohler_03_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../kohler/amazon_sos.csv')

    # cvt = Converter_sos('../havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/share_of_search.json', '../havells_fans/amazon_marketplace_scraping_havells_fans_03_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../havells_fans/amazon_sos.csv')

    # cvt = Converter_sos('../havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/share_of_search.json', '../havells_ac/amazon_marketplace_scraping_havells_ac_04_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../havells_ac/amazon_sos.csv')

    # cvt = Converter_sos('../ghadi/amazon_marketplace_scraping_ghadi_05_03_22/share_of_search.json', '../ghadi/amazon_marketplace_scraping_ghadi_05_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ghadi/amazon_sos.csv')

    # cvt = Converter_sos('../nestle/amazon_marketplace_scraping_nestle_05_03_22/share_of_search.json', '../nestle/amazon_marketplace_scraping_nestle_05_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nestle/amazon_sos.csv')

    # cvt = Converter_sos('../park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/share_of_search.json', '../park_avenue/amazon_marketplace_scraping_park_avenue_19_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../park_avenue/amazon_sos.csv')

    # cvt = Converter_sos('../bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/share_of_search.json', '../bajaj_mixer/amazon_marketplace_scraping_bajaj_mixer_25_03_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../bajaj_mixer/amazon_sos.csv')

    # cvt = Converter_sos('../blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/share_of_search.json', '../blue_heaven/amazon_marketplace_scraping_blue_heaven_05_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../blue_heaven/amazon_sos.csv')

    # cvt = Converter_sos('../nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/share_of_search.json', '../nature_essence/amazon_marketplace_scraping_nature_essence_05_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nature_essence/amazon_sos.csv')

    # cvt = Converter_sos('../eno/amazon_marketplace_scraping_eno_12_04_22/share_of_search.json', '../eno/amazon_marketplace_scraping_eno_12_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../eno/amazon_sos.csv')

    # cvt = Converter_sos('../nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/share_of_search.json', '../nescafe_coffee_maker/amazon_marketplace_scraping_nescafe_coffee_maker_12_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nescafe_coffee_maker/amazon_sos.csv')

    # cvt = Converter_sos('../nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/share_of_search.json', '../nescafe_gold/amazon_marketplace_scraping_nescafe_gold_12_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nescafe_gold/amazon_sos.csv')

    # cvt = Converter_sos('../ceregrow/amazon_marketplace_scraping_ceregrow_13_04_22/share_of_search.json', '../ceregrow/amazon_marketplace_scraping_ceregrow_13_04_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../ceregrow/amazon_sos.csv')

    # cvt = Converter_sos('../nestle_cornflakes/amazon_marketplace_scraping_nestle_cornflakes_15_05_22/share_of_search.json', '../nestle_cornflakes/amazon_marketplace_scraping_nestle_cornflakes_15_05_22/sos_data.json')
    # cvt.convert()
    # cvt.save('../nestle_cornflakes/amazon_sos.csv')

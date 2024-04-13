import pandas as pd


class FileHelper:
    """
    A class used to return a list of Urls of a file.
    """

    def get_urls(file):
        """
        Parameters
        ----------
        file : str
            location of the file

        Returns
        -------
        list
            a list of Urls of the column with name URL of the file
        """

        df = pd.read_csv(file)
        Urls = df["URL"]
        return list(Urls)

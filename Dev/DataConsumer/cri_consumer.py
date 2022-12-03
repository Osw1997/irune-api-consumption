import pandas as pd

class CRIConsumer:
    def __init__(self, path_cri_file="..\..\data\CRI_2019.csv") -> None:
        self.path_cri_file = path_cri_file
        self.df_cri = None
        self.load_file()
        self.list_countries = self.df_cri["Country"].to_list()

    def load_file(self):
        self.df_cri = pd.read_csv(self.path_cri_file)
        self.df_cri["Country"] = self.df_cri["Country"].str.upper()

    def get_cri(self, country=None):
        if country is None:
            raise ValueError("[get_cri][ERROR] country argument is necessary for the query. Please provide it.")

        if country.upper() not in self.list_countries:
            raise ValueError(f"[get_cri][ERROR] Country {country} is not available in the CRI list")

        cri_value = float(self.df_cri[self.df_cri["Country"] == country.upper()]["CRI score"])

        return cri_value

# TEST
path_cri_file = "data\CRI_2019.csv"
cri = CRIConsumer(path_cri_file=path_cri_file)

countries = ["Mexico", "Albania", "Atlantis"]
for country in countries:
    cri_value = cri.get_cri(country=country)
    print(f">>> CRI score for {country} is {cri_value}")

import pandas as pd
import os

class WGIConsumer:
    def __init__(self, path_wgi_folder="..\..\data", file="clean_wgi.csv"):
        self.path_wgi_folder = path_wgi_folder
        self.file = file
        self.df_wgi = None

        self.clean_file(force_overriding=False)
        self.load_file()

    def clean_file(self, force_overriding=False):
        """
        Function that cleans the file that comes from a excel file (wgidataset.xlsx) and its respective sheet (political and stability)
        Args:
            file: Filename that will contain the clean wgi information 
            force: True if override current version (defualt: False)
        """

        # Return if file does not exist or if force_overriding is False
        file_path = f"{self.path_wgi_folder}\{self.file}"
        if os.path.exists(file_path) and not force_overriding:
            print(f"File {self.file} already exists in {self.path_wgi_folder} folder. Skipping rest of clean_file function")
            return None

        # PRocess data
        path_raw_wgi_file = f"{self.path_wgi_folder}\wgi_political_stability.csv"
        df = pd.read_csv(path_raw_wgi_file)
        # Take only data from csv
        table = df[12:]
        table = table.reset_index(drop=True)
        # Columns to get rid of
        subheader = table.iloc[1].to_list()
        columns_index_to_remove = [index_col for index_col, column in enumerate(subheader) if column in ["StdErr", "NumSrc", "Rank", "Lower", "Upper"]]
        table = table.drop(table.columns[columns_index_to_remove], axis=1, inplace=False)

        # get columns to use
        columns = table.iloc[0].to_list()
        columns[0], columns[1] = "Country/Territory", "Code"
        table.columns = columns
        # FRom 2 until the end because we remove the "header" and the extra header (Estimate, StdErr, NumSrc, Rank, Lower, Upper, Estimate again, ...)
        table = table[2:] 
        table = table.reset_index(drop=True)

        # Use "melt" method for converting columns to rows
        # melt_table = table.melt(id_vars=["Country/Territory", "Code"], var_name="Year", value_name="Estimate")
        melt_table = table.melt(
                        id_vars=["Country/Territory", "Code"],
                        var_name="Year",
                        value_name="Value"
                    )
        # Row count: 23 years and 214 countries -> 4,922 entries
        melt_table.to_csv(file_path, index=False)
        return None

    def load_file(self):
        self.df_wgi = pd.read_csv(f"{self.path_wgi_folder}\{self.file}")

    def query_data(self, year=None, country=None):
        """
        Function that returns geopolitical instability value given country and year parameters
        Args:
            year:
            country:
        Returns:
            geopolitical instability value if exists
        """
        gi_index = None

        if country is None:
            raise ValueError("[query_data][ERROR] country argument is necessary for the query. Please provide it.")

        if year is None:
            raise ValueError("[query_data][ERROR] year argument is necessary for the query. Please provide it.")
        

        valid_row = (self.df_wgi["Year"] == year) & (self.df_wgi["Country/Territory"] == country)

        try:
            gi_index = self.df_wgi[valid_row]["Value"].iloc[0]
        except Exception:
            pass

        return gi_index  

# TESTS
path_wgi_folder = "data"
wgi = WGIConsumer(path_wgi_folder=path_wgi_folder)

year, country = 2014, "Mexico"
gi_value = wgi.query_data(year=year, country=country)
print(f"Commercial value for [{year}, {country}] is {gi_value}")

# NOTA 9/dic/2022: EL estimado va de +-2.5 --> hay que normalizarlo.
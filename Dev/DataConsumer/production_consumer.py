from numpy import product
import pandas as pd
import os

class ProductionConsumer:
    def __init__(self, path_production_folder="..\..\data"):
        self.path_production_folder = path_production_folder
        self.raw_file = "Production.xlsx"
        self.melted_file = "Production_one_file.csv"
        self.df_production = None

        self.clean_file()
        self.load_file()

    def clean_file(self, force_overriding=False):
        """
        Function that reads sheets from "Production.xlsx" file, melts it into the schema [Product, Country, Year, Value] and saves it as a single CSV.
        Args:
            force_overriding: True if override current version (defualt: False)
        """

        # Return if file does not exist or if force_overriding is False
        file_path = f"{self.path_production_folder}\{self.melted_file}"
        if os.path.exists(file_path) and not force_overriding:
            print(f"[LOG] File {self.melted_file} already exists in {self.path_production_folder} folder. Skipping rest of clean_file function")
            return None

        metadata = pd.ExcelFile(f"{self.path_production_folder}\{self.raw_file}")
        sheets = metadata.sheet_names

        # Iterate over all the sheets and melt sheets into a table whose schema is [Product, Country, Year, Value]
        # and concatenate it to master dataframe
        master_df = pd.DataFrame({'Year': [], 'Country': [], 'Value': [], 'Product': []})
        for sheet in sheets:
            print(f"[LOG] Melting Sheet {sheet}")
            df = pd.read_excel(f"{self.path_production_folder}\{self.raw_file}", sheet_name=sheet)
            melt_df = df.melt(
                id_vars="Year",
                var_name="Country",
                value_name="Value"
            )
            melt_df['Product'] = sheet
            master_df = pd.concat([master_df, melt_df])
        
        # Save it as CSV
        master_df.to_csv(f"{self.path_production_folder}\{self.melted_file}", index=False)

        return None

    def load_file(self):
        self.df_production = pd.read_csv(f"{self.path_production_folder}\{self.melted_file}")

    def query_data(self, product=None, year=None, country=None):
        """
        Function that returns commercial value given product, year and country parameters
        Args:
            product:
            year:
            country:
        Returns:
            Commercial value if exists
        """
        commercial_value = None

        if product is None:
            raise ValueError("[query_data][ERROR] product argument is necessary for the query. Please provide it.")
        
        if year is None:
            raise ValueError("[query_data][ERROR] year argument is necessary for the query. Please provide it.")
        
        if country is None:
            raise ValueError("[query_data][ERROR] country argument is necessary for the query. Please provide it.")

        valid_row = (self.df_production["Product"] == product) & (self.df_production["Year"] == year) & (self.df_production["Country"] == country)

        try:
            commercial_value = self.df_production[valid_row]["Value"].iloc[0]
        except Exception:
            pass

        return commercial_value  

# # TESTS
# path_production_folder = "data"
# pc = ProductionConsumer(path_production_folder=path_production_folder)

# product, year, country = "Antimony", 2014, "Mexico"
# commercial_value = pc.query_data(product=product, year=year, country=country)
# print(f"Commercial value for [{product}, {year}, {country}] is ${commercial_value}")


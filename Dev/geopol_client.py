from unittest import result
from DataConsumer.comtrade_consumer import ComtradeConsumer
import pandas as pd

class GeopolriskConsumer:
    def __init__(self) -> None:
        # self.master_df = pd.read_csv("../data/hhi_wgi_prod_only_important_cols.csv")
        self.master_df = pd.read_csv("../data/hhi_production_egsehi_conformed_table.csv")

        # Lista de elementos de interés
        classification_codes = {
            "Antimony": 261710,
            "Asbestos": 2524,
            "Barytes": 2511,
            "Bismuth": 8106,
            "Cadmium": 8107,
            "Chromium": 2610,
            "Coal": 2701,
            "Cobalt": 810520,
            "Copper": 2603,
            "Gold": 7108,
            "Graphite": 2504,
            "Iron": 2601,
            "Lead": 2607,
            "Lithium": 283691,
            "Magnesite": 251910,
            "Magnesium": 251910,
            "Manganese": 2602,
            "Mercury": 280540,
            "Molybdenum": 2613,
            "Natural gas": 271111,
            "Nickel": 2604,
            "Petroleum": 2709,
            "Rare earth": 2846,
            "Silver": 261610,
            "Tin": 2609,
            "Titanium": 2614,
            "Tungsten": 2611,
            "Uranium": 261210,
            "Zinc": 2608,
            "Zirconium": 261510,
            "Lithium-ion batteries": 850760 # igual que Litio?
        }
        self.classification_codes = {key.upper(): val for key, val in classification_codes.items()}
        
        self.ctc = ComtradeConsumer()

        # Obtenemos lista de países disponibles para reporter (igual que partner (?))
        self.list_reporter_countries = self.ctc.get_reporter_countries()
        # Remueve "all" porque marca error --> indice 0
        self.list_reporter_countries = self.list_reporter_countries[1:]

    def get_country_code(self, country):
        for dict_country in self.list_reporter_countries:
            if dict_country["text"].upper() == country.upper():
                return dict_country["id"]
        raise ValueError(f"Country {country} does not exist.")

    def get_country_code(self, country):
        for dict_country in self.list_reporter_countries:
            if dict_country["text"].upper() == country.upper():
                return dict_country["id"]
        raise ValueError(f"Country {country} does not exist.")

    def query_comtrade_api(self, country, product, ps):
        r = self.get_country_code(country)
        cc = self.classification_codes[product.upper()]

        datasets = self.ctc.get_trade_value_imports(
                                type='C', freq='A', px='HS',
                                ps=ps, r=r, p='ALL', rg=1, cc=cc)

        index_world = None
        list_partner, list_pt3ISO, list_tradeValues, list_year = [], [], [], []
        for index, dataset in enumerate(datasets):
            if dataset["ptTitle"] == "World":
                index_world = index
                continue
            list_partner.append(dataset["ptTitle"])
            list_pt3ISO.append(dataset["pt3ISO"])
            # list_tradeValues.append(float(dataset["TradeValue"])) # Debe ser "TradeQuantity"
            list_tradeValues.append(float(dataset["TradeQuantity"])) # Debe ser "TradeQuantity"
            list_year.append(dataset["yr"])

        # Get F upper case
        F_tradeValue = float(datasets[index_world]["TradeValue"])

        temp_df = pd.DataFrame({
            "Reporter": [country.upper()] * (len(datasets) - 1),
            "Partner": list_partner,
            "ISO3 Code": list_pt3ISO,
            "Imports": list_tradeValues,
            "Total Imports": [F_tradeValue] * (len(datasets) - 1),
            "YearLog": list_year
        })

        return temp_df
    
    # def get_geopol_risk_deprecated(self, list_tuples):

    #     total_result_df = pd.DataFrame({
    #         "Year": [], "Country": [], "Product": [], "iso3code": [],
    #         "hhi": [], "Stability value": [], "CRI score": [],
    #         "Domestic Production Value": [], "Reporter": [], "Partner":	[],
    #         "ISO3 Code": [], "Imports": [], "Total Imports": [], "YearLog":[], 
    #         "Geopolitical Risk": []
    #     })

    #     total_condensed_result_df = pd.DataFrame({
    #         "Product": [], "Country": [], "Year": [], "Geopolitical Risk": []
    #     })

    #     for tuple in list_tuples:
    #         country, product, ps = tuple[0], tuple[1], tuple[2]

    #         # Filter FACT table with user parameters
    #         valid_rows = (self.master_df["Country"] == country.upper()) & (self.master_df["Product"] == product.upper()) & (self.master_df['Year'] == ps)
    #         filtered_fact_df = self.master_df[valid_rows]

    #         # Query Comtrade API
    #         comtrade_df = self.query_comtrade_api(country, product, ps)

    #         # Merge filtered FACT table and comtrade result table
    #         result_df = pd.merge(left=filtered_fact_df, right=comtrade_df, how="left", left_on=["Year", "Country"], right_on=["YearLog", "Reporter"])
    #         # Work out the Geopolitical Risk
    #         result_df["Geopolitical Risk"] = result_df["hhi"] * (result_df["Stability value"] * result_df["Imports"] / (result_df["Domestic Production Value"] + result_df["Total Imports"]))

    #         condensed_result_df = result_df[["Product", "Country", "Year", "Geopolitical Risk"]].groupby(by=["Product", "Country", "Year"]).sum()

    #         total_result_df = pd.concat([total_result_df, result_df])
    #         total_condensed_result_df = pd.concat([total_condensed_result_df, condensed_result_df])
        
        return total_result_df, total_condensed_result_df

    def get_geopol_risk(self, list_tuples):
        total_result_df = pd.DataFrame({
            "Year": [], "Country": [], "Domestic Production Value (P_AC)": [], "Product": [], "ISO3": [], 
            "value_economic": [], "no_value_economic": [], "value_governance": [], "no_value_governance": [], "value_social": [], "no_value_social": [],
            "value_ecosystems": [], "no_value_ecosystems": [], "value_habitat": [], "no_value_habitat": [], "value_infrastructure": [], "no_value_infrastructure": [],
            "EGSEHI": [], "EGSEHI_6root": [], "Value": [], "reporterCode": [], "Total_value_YearProduct": [], "Share in % (production)": [], "Share HHI Production": [], "HHI_production": [],
            "classificationCode": [], "classificationSearchCode": [], "cmdCode": [], "cmdDesc": [], "netWgt": [], "primaryValue": [], "Total_netWgt_YearProduct": [],
            "Share in % (exports)": [], "Share HHI Exports": [], "HHI_exports": [], "Partner": [], "ISO3 Code": [], "Imports": [], "Total Imports": [],
            "Geopolitical Risk Production": [], "Geopolitical Risk Exports": []
        })

        total_condensed_result_df = pd.DataFrame({
            "Product": [], "Country": [], "Year": [], "cmdCode": [], 
            "Geopolitical Risk Production": [], "Geopolitical Risk Exports": []
        })


        for tuple in list_tuples:
            country, product, ps = tuple[0], tuple[1], tuple[2]

            # Filter FACT table with user parameters
            valid_rows = (self.master_df["Country"] == country.upper()) & (self.master_df["Product"] == product.upper()) & (self.master_df['Year'] == ps)
            filtered_fact_df = self.master_df[valid_rows]

            # Query Comtrade API
            comtrade_df = self.query_comtrade_api(country, product, ps)

            # Merge filtered FACT table and comtrade result table
            result_df = pd.merge(left=filtered_fact_df, right=comtrade_df, how="left", left_on=["Year", "Country"], right_on=["YearLog", "Reporter"])
            # Work out the Geopolitical Risk
            result_df["Geopolitical Risk Production"] = result_df["HHI_production"] * (result_df["EGSEHI_6root"] * result_df["Imports"] / (result_df["Total_value_YearProduct"] + result_df["Total Imports"]))
            result_df["Geopolitical Risk Exports"] = result_df["HHI_exports"] * (result_df["EGSEHI_6root"] * result_df["Imports"] / (result_df["Total_value_YearProduct"] + result_df["Total Imports"]))

            # Query only "important" columns
            result_df = result_df[list(total_result_df.keys())]


            condensed_result_df = result_df[["Product", "Country", "Year", "cmdCode", "Geopolitical Risk Production", "Geopolitical Risk Exports"]].groupby(by=["Product", "Country", "Year", "cmdCode"]).sum()

            total_result_df = pd.concat([total_result_df, result_df])
            total_condensed_result_df = pd.concat([total_condensed_result_df, condensed_result_df])
        
        return total_result_df, total_condensed_result_df
    
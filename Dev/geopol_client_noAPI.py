import pandas as pd

class GeopolriskConsumer:
    def __init__(self) -> None:
        self.master_df = pd.read_csv("../data/all_consolidated_data/all_consolidated_data_noWorld.csv")

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

    def get_geopol_risk(self, list_tuples):
        total_result_df = pd.DataFrame({
            "Year": [], "Country": [], "Domestic Production Value (P_AC)": [], "Product": [], "ISO3": [], 
            "value_economic": [], "no_value_economic": [], "value_governance": [], "no_value_governance": [], "value_social": [], "no_value_social": [],
            "value_ecosystems": [], "no_value_ecosystems": [], "value_habitat": [], "no_value_habitat": [], "value_infrastructure": [], "no_value_infrastructure": [],
            "value_governance_isolated": [], "no_value_governance_isolated": [],
            "EGSEHI": [], "EGSEHI_6root": [], "Value": [], "reporterCode": [], "Total_value_YearProduct": [], "Share in % (production)": [], "Share HHI Production": [], "HHI_production": [],
            "classificationCode": [], "cmdCode": [], "cmdDesc": [], "netWgt": [], "primaryValue": [], "Total_netWgt_YearProduct": [],
            "Share in % (exports)": [], "Share HHI Exports": [], "HHI_exports": [], "partnerDesc_import": [], "partnerISO_import": [], "netWgt_import": [], "netWgt_total_import": [],
            "Geopolitical Risk Production": [], "Geopolitical Risk Exports": [], "Geopolitical Risk Only Political Stability": [],
            "Geopolitical Risk Production NO-ROOT": [], "Geopolitical Risk Exports NO-ROOT": []
        })

        total_condensed_result_df = pd.DataFrame({
            "Product": [], "Country": [], "Year": [], "cmdCode": [], 
            "Geopolitical Risk Production": [], "Geopolitical Risk Exports": [], "Geopolitical Risk Only Political Stability": [],
            "Geopolitical Risk Production NO-ROOT": [], "Geopolitical Risk Exports NO-ROOT": []
        })

        for tuple in list_tuples:
            country, product, ps = tuple[0], tuple[1], tuple[2]

            country_rows = self.master_df["Country"] == country.upper()
            product_rows = self.master_df["cmdCode"] == self.classification_codes[product.upper()]
            year_rows = self.master_df["Year"] == ps
            result_df = self.master_df[(country_rows) & (product_rows) & (year_rows)]

            # # Remove "World" entry
            # result_df = result_df[result_df["partnerDesc_import"] != "World"]

            # Query only "important" columns
            result_df = result_df[list(total_result_df.keys())]
            total_result_df = pd.concat([total_result_df, result_df])

            # summarized results
            condensed_result_df = result_df[["Product", "Country", "Year", "cmdCode", 
                                                "Geopolitical Risk Production", "Geopolitical Risk Exports", "Geopolitical Risk Only Political Stability",
                                                "Geopolitical Risk Production NO-ROOT", "Geopolitical Risk Exports NO-ROOT"]
                                            ].groupby(by=["Product", "Country", "Year", "cmdCode"], as_index=False).sum()
            total_condensed_result_df = pd.concat([total_condensed_result_df, condensed_result_df])

        return total_result_df, total_condensed_result_df
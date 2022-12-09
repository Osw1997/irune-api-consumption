import requests

class WitsConsumer:
    def __init__(self):
        pass

    def get_hhi_index(self, iso3code=None, year=None) -> float:
        """
            Function that returns HHI acorrding with arguments provided.
            Args:
                iso3code: ISO code associated to the country/region user wants to query.
                year: year that user wants to query.
                    (default is ALL)
            
            Returns:
                HHI index if exists. None if not.
        """
        if iso3code is None:
            raise ValueError("[get_hhi_index][ERROR] iso3code argument is necessary for the query. Please provide it.")
        
        if year is None:
            raise ValueError("[get_hhi_index][ERROR] year argument is necessary for the query. Please provide it.")

        url = f"http://wits.worldbank.org/API/V1/SDMX/V21/datasource/tradestats-trade/reporter/{iso3code}/year/{year}/indicator/HH-MKT-CNCNTRTN-NDX?format=JSON"
        
        hhi_index = None
        try:
            petition = requests.get(url=url).json()            
            # Get HHI index from json
            hhi_index = petition["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]["0"][0]
        except requests.ConnectionError as ce:
            print(f"[get_hhi_index][ERROR] Something happened in the request connection: {ce}")
        except Exception as e:
            print(f"[get_hhi_index][ERROR] Something happened: {e}")

        return hhi_index

    def get_list_iso3codes(self):
        """
            Function that an XML as string of ISO 3 codes.
            Args:            
            Returns:
                XML that contains ISO 3 codes.
        """
        url = "http://wits.worldbank.org/API/V1/wits/datasource/trn/country/ALL"
        try:
            petition = requests.get(url=url).text
        except requests.ConnectionError as ce:
            print(f"[get_list_iso3codes][ERROR] Something happened in the request connection: {ce}")
        except Exception as e:
            print(f"[get_list_iso3codes][ERROR] Something happened: {e}")
        
        return petition

# # TESTS

# # Print HHI index given a country/region and year.
# wc = WitsConsumer()
# iso3code, year = "mex", 2018
# print(f"--> HHI index for {iso3code} and year {year} is {wc.get_hhi_index(iso3code=iso3code, year=year)}", end="\n\n")

# iso3code, year = "mex", 2072
# print(f"HHI index for {iso3code} and year {year} is {wc.get_hhi_index(iso3code=iso3code, year=year)}", end="\n\n")

# # # Print iso3 codes.
# # print(wc.get_list_iso3codes())

# # TODO: Pedir lista-rango parámetros a usar

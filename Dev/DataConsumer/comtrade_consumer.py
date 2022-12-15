import requests
import json

class ComtradeConsumer:
    def __init__(self):
        pass

    def get_trade_value_imports(self, type=None, freq=None, px=None, ps=None, r=None, p=None, rg=None, cc=None) -> list:
        """
            Function that returns value of imports of resource {{cc}} from country {{r}} to country {{p}}
            Args:
                type: trade data typetrade data type. [C: commodities, S: services]
                freq: data set frequency [M: monthly, A annual]
                px: classification. [HS: Harmonized System, and more :) ]
                ps: time period. Format: YYYYMM
                r: Reporting area. Default: 0 (world wide). Full list --> https://comtrade.un.org/Data/cache/reporterAreas.json
                p: partner area. Default: all. List --> https://comtrade.un.org/Data/cache/partnerAreas.json
                rg: trade regime / trade flow. [default: all, 1:imports, 2:exports]
                cc: Classification code.
            
            Returns:
                
        """
        if type is None:
            raise ValueError("[get_trade_value_imports][ERROR] type argument is necessary for the query. Please provide it.")
        
        if freq is None:
            raise ValueError("[get_trade_value_imports][ERROR] freq argument is necessary for the query. Please provide it.")
        
        if px is None:
            raise ValueError("[get_trade_value_imports][ERROR] px argument (classification) is necessary for the query. Please provide it.")
        
        if ps is None:
            raise ValueError("[get_trade_value_imports][ERROR] ps argument (time perdiod --> YYYYMM) is necessary for the query. Please provide it.")

        if r is None:
            raise ValueError("[get_trade_value_imports][ERROR] r argument (reporting area) is necessary for the query. Please provide it.")
        
        if p is None:
            raise ValueError("[get_trade_value_imports][ERROR] p argument (partner area) is necessary for the query. Please provide it.")
        
        if rg is None:
            raise ValueError("[get_trade_value_imports][ERROR] rg argument (trade regime/trade flow) is necessary for the query. Please provide it.")
        
        if cc is None:
            raise ValueError("[get_trade_value_imports][ERROR] c argument (classification code) is necessary for the query. Please provide it.")

        url = f"https://comtrade.un.org/api/get?type={type}&freq={freq}&px={px}&ps={ps}&r={r}&p={p}&rg={rg}&cc={cc}"
        

        trade_value = []
        try:
            petition = requests.get(url=url).json()
            # Get tradee value from petition
            trade_value = petition["dataset"]
        except requests.ConnectionError as ce:
            print(f"[get_trade_value_imports][ERROR] Something happened in the request connection: {ce}")
        except Exception as e:
            print(f"[get_trade_value_imports][ERROR] Something happened: {e}")

        return trade_value

    def get_reporter_countries(self):
        list_countries = None
        try:
            petition = requests.get("https://comtrade.un.org/Data/cache/reporterAreas.json")
            petition_decoded = json.loads(petition.content.decode("utf-8-sig"))
            list_countries = petition_decoded["results"]
        except Exception as e:
            print(f"[get_reporter_countries][ERROR] Something happened: {e}")
            
        
        return list_countries


# # TESTS

# # Print HHI index given a country/region and year.
# comtrade = ComtradeConsumer()
# type, freq, px, ps, r, p, rg, cc = 'C', 'M', 'HS', '201501', '398', '0', '1', 'TOTAL'
# print(f"Trave value for {type}, {freq}, {px}, {ps}, {r}, {p}, {rg}, {cc} is {comtrade.get_trade_value_imports(type=type, freq=freq, px=px, ps=ps, r=r, p=p, rg=rg, cc=cc)}", end="\n\n")

# type, freq, px, ps, r, p, rg, cc = 'C', 'M', 'HS', '202501', '398', '0', '1', 'TOTAL'
# print(f"Trave value for {type}, {freq}, {px}, {ps}, {r}, {p}, {rg}, {cc} is {comtrade.get_trade_value_imports(type=type, freq=freq, px=px, ps=ps, r=r, p=p, rg=rg, cc=cc)}", end="\n\n")

# # TODO: Pedir lista-rango parámetros a usar

# # Get list of reporter countries
# comtrade = ComtradeConsumer()
# lca = comtrade.get_reporter_countries()
# print(type(lca))
# print(lca)

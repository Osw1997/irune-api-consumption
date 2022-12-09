from DataConsumer.comtrade_consumer import ComtradeConsumer
from DataConsumer.wits_consumer import WitsConsumer

class DataLoader:
    def __init__(self) -> None:
        self.witsconsumer = WitsConsumer()
        

from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads

from abc import ABC, abstractmethod

class BaseConsumer(ABC):
    
    @abstractmethod
    def __init__(self) -> None: ...
    
    @abstractmethod
    def show_consumer(self, file): ...

    @abstractmethod
    def create_consumer(self): ...

class Consumer(BaseConsumer):

    def __init__(self, IP):
        self.IP = IP

    def create_consumer(self):
        consumer = KafkaConsumer(
                    'demo_test',
                    bootstrap_servers=[self.IP], 
                    value_deserializer=lambda x: loads(x.decode('utf-8')))
        return consumer
    
    def show_consumer(self, consumer):
        try:
            for c in consumer:
                print (c.value)
        except Exception as e:
            print(f'{e} please check the data stream')
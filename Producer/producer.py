from abc import ABC, abstractmethod
from kafka import KafkaProducer
from json import dumps
from .Map.mapper import Mapper as Map
from time import sleep
class BaseProducer(ABC):
    
    @abstractmethod
    def __init__(self) -> None: ...
    
    @abstractmethod
    def run_producer(self, file): ...

    @abstractmethod
    def create_producer(self): ...

class Producer(BaseProducer):

    def __init__(self, IP, batch_size, start_index) -> None:
        self.IP = IP
        self.batch_size = batch_size
        self.start_index = start_index

    def create_producer(self):
        producer = KafkaProducer(
            bootstrap_servers=[self.IP],
            api_version=(0,11,5), 
            value_serializer=lambda x: dumps(x).encode('utf-8')
            )
        return producer
    def run_producer(self, file):
        producer = self.create_producer()
        map_1 = Map(file)
        ddf = map_1.read_file()
        while self.start_index < len(ddf):

            end_index = min(self.start_index + self.batch_size, len(ddf))
            # Use loc for row selection
            batch_df = ddf.loc[self.start_index:end_index]

            # Map Phase: Apply map function to the batch
            intermediate_data = map_1.map_function(batch_df)

            # Shuffling and Sorting (not explicitly done since data is not distributed)

            # Reduce Phase: Aggregate the data for each date
            final_result = {intermediate_data[0]: intermediate_data[1]}

            # Send the final result to Kafka
            producer.send('demo_test', value=final_result)

            self.start_index += self.batch_size
            sleep(1)


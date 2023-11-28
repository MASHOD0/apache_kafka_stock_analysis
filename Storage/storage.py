from abc import ABC, abstractmethod
import json
from s3fs import S3FileSystem
class BaseStorage(ABC):
    
    @abstractmethod
    def __init__(self) -> None: ...
    
class Storage(BaseStorage):
    def __init__(self, location) -> None:
        self.location = location

    def store_to_aws_s3(self, consumer):
        for count, i in enumerate(consumer):
            with S3FileSystem.open(self.location.format(count), 'w') as file:
                json.dump(i.value, file) 
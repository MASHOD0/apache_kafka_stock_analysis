from Consumer.consumer import Consumer
from cred import IP, location
import Storage.storage

if __name__=='__main__':
    consumer = Consumer(IP)
    c = consumer.create_consumer()
    consumer.show_consumer(c)
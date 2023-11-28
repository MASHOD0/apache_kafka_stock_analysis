from Producer.producer import Producer
from cred import IP, location


file = "indexProcessed.csv"
batch_size = 1000
start_index = 0

if __name__=='__main__':
    print('producer stream started .....')
    try:
        prod = Producer(IP=IP, batch_size=batch_size, start_index=start_index)
        prod.run_producer(file=file)
    except Exception as e:
        print(e)
        print('stream_ended')

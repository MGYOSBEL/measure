from data.get import GetData
from amqp.publisher import Publisher
from amqp.consumer import Consumer
import os, time

def read_from_api_and_publish():
    # Reading the URL's from the environment
    api_url = os.environ["DATA_API_URL"]
    amqp_url = os.environ["CLOUD_AMQP_URL"]

    # Obtaining the rates from the API
    print("Obtaining data from API and send it to the queue...")
    rates = GetData(api_url)
    print("Data fetched: ")
    print(rates.data)

    # Sending the rates to the queue
    print("Sending data to the AMQP broker...")
    publisher = Publisher(url=amqp_url, queue="Yosbel")
    publisher.publish(rates.data)
    publisher.close_connection()
    print("Data succesfully sent...")


def data_processing_function(data):
    print("Processing data...")
    time.sleep(5)
    print(data)
    print("Processing data SUCCESFULLY FINISHED!")



def run_worker_for_consuming_data():
    amqp_url = os.environ["CLOUD_AMQP_URL"]

    consumer = Consumer(url=amqp_url, queue="Yosbel", callback=data_processing_function)
    consumer.consume()
    consumer.close_connection()


def main():
    for i in range(10):
        read_from_api_and_publish()
    
    run_worker_for_consuming_data()

if __name__ == "__main__":
    main()

import pika
import json

class Publisher:

    def __init__(self, url, queue):
        self.queue = queue
        self.url = url
        self.init_connection(url)


    def init_connection(self, url):
        params = pika.URLParameters(url)
        params.socket_timeout = 5
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP
    

    def publish(self, data):
        channel = self.connection.channel() # start a channel
        channel.basic_publish(
                            exchange = '',
                            routing_key = self.queue,
                            body = json.dumps(data),
                            properties = pika.BasicProperties(
                                delivery_mode = 2
                            ))

                            
    def close_connection(self):
        self.connection.close()


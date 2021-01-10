import pika
import json


class Consumer:

    def __init__(self, url, queue, callback):
        self.queue = queue
        self.url = url
        self.data_processing_callback = callback
        self.init_connection(url)
        print(self.url, self.data_processing_callback, self.queue)


    def init_connection(self, url):
        params = pika.URLParameters(url)
        params.socket_timeout = 5
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP
    
    
    def consume(self):
        channel = self.connection.channel() # start a channel
        channel.basic_consume(
                        self.queue,
                        lambda ch, method, properties, body: self._callback(ch, method, properties, body)
                      )
        channel.start_consuming()


    def _callback(self, ch, method, properties, body):
        self.data_processing_callback(body)
        ch.basic_ack(delivery_tag = method.delivery_tag)


    def close_connection(self):
        self.connection.close()



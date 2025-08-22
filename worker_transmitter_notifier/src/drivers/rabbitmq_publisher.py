import json
import pika
from worker_transmitter_notifier.src.configs import RABBITMQ_CONFIGS

class RabbitmqPublisher:
    def __init__(self) -> None:
        self.__host = RABBITMQ_CONFIGS["HOST"]
        self.__port = RABBITMQ_CONFIGS["PORT"]
        self.__username = RABBITMQ_CONFIGS["USERNAME"]
        self.__password = RABBITMQ_CONFIGS["PASSWORD"]
        self.__vhost = RABBITMQ_CONFIGS["VHOST"]
        self.__exchange = RABBITMQ_CONFIGS["EXCHANGE"]
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            virtual_host=self.__vhost,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )

        channel = pika.BlockingConnection(connection_parameters).channel()
        return channel

    def send_message(self, body: dict):
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key="",
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

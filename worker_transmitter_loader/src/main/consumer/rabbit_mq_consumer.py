# pylint:disable=R0902
import pika
from worker_transmitter_loader.src.configs import RABBITMQ_CONFIGS
from worker_transmitter_loader.src.drivers.logger_handler import logger_handler

class RabbitmqConsumer:
    def __init__(self, callback) -> None:
        self.__host = RABBITMQ_CONFIGS["HOST"]
        self.__port = RABBITMQ_CONFIGS["PORT"]
        self.__username = RABBITMQ_CONFIGS["USERNAME"]
        self.__password = RABBITMQ_CONFIGS["PASSWORD"]
        self.__queue = RABBITMQ_CONFIGS["QUEUE"]
        self.__vhost = RABBITMQ_CONFIGS["VHOST"]
        self.__heartbeat = 10000
        self.__callback = callback
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            virtual_host=self.__vhost,
            heartbeat=self.__heartbeat,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )

        channel = pika.BlockingConnection(connection_parameters).channel()
        channel.queue_declare(
            queue=self.__queue,
            durable=True,
            arguments={"x-queue-type":"quorum"}
        )
        channel.basic_consume(
            queue=self.__queue,
            auto_ack=True,
            on_message_callback=self.__callback
        )

        return channel

    def start(self):
        msg = f'RabbitMQ is now listening to the queue {RABBITMQ_CONFIGS["QUEUE"]} at server {RABBITMQ_CONFIGS["HOST"]}'
        logger_handler.log(msg)
        self.__channel.start_consuming()

from worker_transmitter_loader.src.main.consumer.rabbit_mq_consumer import RabbitmqConsumer
from worker_transmitter_loader.src.main.callbacks.consumer_callback import consumer_calback

if __name__=="__main__":

    consumer = RabbitmqConsumer(consumer_calback)
    consumer.start()

from worker_transmitter_notifier.src.drivers.rabbitmq_publisher import RabbitmqPublisher

publisher = RabbitmqPublisher()
publisher.send_message({ "olaMundo": "Aqui" })

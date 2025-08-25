#pylint:disable=W0613
import json
from worker_transmitter_loader.src.drivers.logger_handler import logger_handler

def consumer_calback(ch, method, properties, body) -> None:

    message = json.loads(body.decode("utf-8"))

    logger_handler.log('Starting a new resume cfop')
    logger_handler.log(message)

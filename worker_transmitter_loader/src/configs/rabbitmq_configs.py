import os

RABBITMQ_CONFIGS = {
    "HOST": os.getenv("RABBITMQ_HOST"),
    "PORT": os.getenv("RABBITMQ_PORT"),
    "USERNAME": os.getenv("RABBITMQ_USERNAME"),
    "PASSWORD": os.getenv("RABBITMQ_PASSWORD"),
    "EXCHANGE": os.getenv("RABBITMQ_EXCHANGE"),
    "QUEUE": os.getenv("RABBITMQ_QUEUE"),
    "VHOST": os.getenv("RABBITMQ_VHOST"),
}

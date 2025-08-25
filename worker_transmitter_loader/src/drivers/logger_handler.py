import logging

class __LoggerHandler:
    def __init__(self):
        self.__logger = self.__setup_logger()

    def __setup_logger(self, name="app_loader_logger", log_file="app_transmitter_loader.log", level=logging.INFO):
        """
        Configura um logger padrão com saída para arquivo e console.

        Args:
            name (str): Nome do logger.
            log_file (str): Nome do arquivo de log.
            level (int): Nível mínimo de log (ex: logging.INFO, logging.DEBUG).
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Evita múltiplos handlers duplicados
        if not logger.handlers:

            # Criação do formato
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

            # Log para arquivo
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Log para console
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def log(self, message: str):
        self.__logger.info(message)

logger_handler = __LoggerHandler()

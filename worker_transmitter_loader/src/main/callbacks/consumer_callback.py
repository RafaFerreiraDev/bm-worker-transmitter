#pylint:disable=W0613
import json
from worker_transmitter_loader.src.drivers.logger_handler import logger_handler
from worker_transmitter_loader.src.controller.files_loader_controller import FilesLoaderController

def consumer_calback(ch, method, properties, body) -> None:

    message = json.loads(body.decode("utf-8"))

    logger_handler.log('Iniciando processo de consumo de mensagens')

    arquivos = message["arquivos"]
    files_loader_controller = FilesLoaderController()

    for file_path_dir in arquivos:
        try:
            #if "Arquivos Entradas e Saídas" in file_path_dir or "TRANSMIT" in file_path_dir: continue
            files_loader_controller.load(file_path_dir)
        except Exception as exception:
            logger_handler.log(f"Erro no arquivo: {file_path_dir}\n\n {exception}")

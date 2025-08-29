from worker_transmitter_notifier.src.configs import SHAREPOINT_CONFIGS
from worker_transmitter_notifier.src.drivers.rabbitmq_publisher import RabbitmqPublisher
from worker_transmitter_notifier.src.drivers.sharepoint_handler import SharepointHandler
from worker_transmitter_notifier.src.drivers.logger_handler import LoggerHandler

class SharepointFilesController:
    def __init__(self):
        self.__base_folder_url = SHAREPOINT_CONFIGS["BASE_FOLDER_URL"]
        self.__rabbitmq_publisher = RabbitmqPublisher()
        self.__sharepoint_handler = SharepointHandler()
        self.__logger_handler = LoggerHandler()
        self.__chunk_size = 3

    def get_files_and_send_to_rabbitmq(self) -> None:
        self.__logger_handler.log(f"Iniciando processo de busca...: {self.__base_folder_url}")

        files_paths = self.__get_files_name()
        self.__logger_handler.log(f"Arquivos encontrados | total {len(files_paths)}")

        self.__logger_handler.log("Iniciando envio de mensagem...")
        self.__send_files_to_rabbitmq_in_chunks(files_paths)

        self.__logger_handler.log("Processo finalizado com sucesso")

    def __get_files_name(self) -> list[str]:
        self.__sharepoint_handler.explore_folder(self.__base_folder_url)
        files_paths = self.__sharepoint_handler.all_file_paths
        return files_paths

    def __send_files_to_rabbitmq_in_chunks(self, files_paths: list[str]) -> None:
        for i in range(0, len(files_paths), self.__chunk_size):
            chunk = files_paths[i:i+self.__chunk_size]
            self.__rabbitmq_publisher.send_message({ "arquivos": chunk })

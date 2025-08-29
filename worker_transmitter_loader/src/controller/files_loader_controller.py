import re
from worker_transmitter_loader.src.drivers.sharepoint_handler import SharepointHandler
from worker_transmitter_loader.src.drivers.s3_handler import S3Uploader
from worker_transmitter_loader.src.drivers.logger_handler import logger_handler

class FilesLoaderController:
    def __init__(self):
        self.__sharepoint_handler = SharepointHandler()
        self.__s3_uploader = S3Uploader()
        self.__loger = logger_handler

    def load(self, file_path_dir: str) -> None:
        self.__loger.log(f"Validando Arquivo: {file_path_dir}")
        formatted_dir = self.__format_file_name(file_path_dir)
        if self.__s3_uploader.object_exists(formatted_dir):
            self.__loger.log(f"✅ Arquivo já existente!")
            return

        self.__loger.log(f"Iniciando leitura do arquivo: {file_path_dir}")
        _, binary_file = self.__get_file_and_verify(file_path_dir)

        formatted_dir = self.__format_file_name(file_path_dir)
        self.__loger.log(f"Enviando arquivo para {formatted_dir}")
        self.__s3_uploader.upload_stream(formatted_dir, binary_file)

        self.__loger.log(f"✅ Upload concluído para arquivo: {file_path_dir}")

    def __get_file_and_verify(self, file_path_dir: str):
        first_line, binary_file = self.__sharepoint_handler.read_file_content(file_path_dir)
        if not binary_file: raise Exception("Arquivo não encontrado!")
        return first_line, binary_file

    def __format_file_name(self, file_path_dir: str):
        nome_arquivo = file_path_dir.split('/')[-1]

        # Extrai o CNPJ do nome do arquivo
        match = re.search(r'_(\d{14})_', nome_arquivo)
        cnpj = match.group(1)
        novo_caminho = f"GPA/47508411/GERENCIAL_SHAREPOINT/{cnpj}/{nome_arquivo}"
        return novo_caminho

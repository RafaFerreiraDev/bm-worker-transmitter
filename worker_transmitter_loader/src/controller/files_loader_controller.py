from worker_transmitter_loader.src.drivers.sharepoint_handler import SharepointHandler
from worker_transmitter_loader.src.drivers.s3_handler import S3Uploader
from worker_transmitter_loader.src.drivers.logger_handler import logger_handler

class FilesLoaderController:
    def __init__(self):
        self.__sharepoint_handler = SharepointHandler()
        self.__s3_uploader = S3Uploader()
        self.__loger = logger_handler

    def load(self, file_path_dir: str) -> None:
        self.__loger.log(f"Iniciando leitura do arquivo: {file_path_dir}")
        first_line, binary_file = self.__get_file_and_verify(file_path_dir)

        formatted_dir = self.__format_file_name(first_line)
        self.__loger.log(f"Enviando arquivo para {formatted_dir}")
        self.__s3_uploader.upload_stream(formatted_dir, binary_file)

        self.__loger.log(f"✅ Upload concluído para arquivo: {file_path_dir}")

    def __get_file_and_verify(self, file_path_dir: str):
        first_line, binary_file = self.__sharepoint_handler.read_file_content(file_path_dir)
        if not binary_file: raise Exception("Arquivo não encontrado!")
        return first_line, binary_file

    def __format_file_name(self, first_line: str):
        if not first_line.startswith("|0000|"):
            raise ValueError("Arquivo SPED inválido: primeira linha não é 0000")

        campos = first_line.strip("|").split("|")
        data_ini = campos[3]
        cnpj = campos[6]
        ie = campos[9]
        ano = data_ini[4:]
        mes = data_ini[2:4]

        return f"EFD_FISCAL_{cnpj}_{ie}_{ano}_{mes}.txt"

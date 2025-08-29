from worker_transmitter_loader.src.drivers.sharepoint_handler import SharepointHandler
from worker_transmitter_loader.src.drivers.s3_handler import S3Uploader
from worker_transmitter_loader.src.drivers.logger_handler import logger_handler

class XlsxLoaderController:
    def __init__(self):
        self.__sharepoint_handler = SharepointHandler()
        self.__s3_uploader = S3Uploader()
        self.__loger = logger_handler

    def load(self, file_path_dir: str) -> None:
        self.__loger.log(f"Validando Arquivo: {file_path_dir}")
        formatted_dir = self.__format_file_name(file_path_dir)
        #if self.__s3_uploader.object_exists(formatted_dir):
        #    self.__loger.log(f"✅ Arquivo já existente!")
        #    return


        file_obj = self.__sharepoint_handler.download_xlsx_file(file_path_dir)

        self.__s3_uploader.upload_fileobj(file_obj, formatted_dir)

    def __format_file_name(self, nome_arquivo: str):
        novo_caminho = f"GPA/47508411/GERENCIAL_NFE/{nome_arquivo}"
        return novo_caminho

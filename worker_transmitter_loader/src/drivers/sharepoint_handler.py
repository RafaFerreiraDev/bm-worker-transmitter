import io
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.files.file import File

from worker_transmitter_notifier.src.configs import SHAREPOINT_CONFIGS


class SharepointHandler:
    def __init__(self):
        self.__credentials = ClientCredential(
            SHAREPOINT_CONFIGS["CLIENT_ID"],
            SHAREPOINT_CONFIGS["CLIENT_SECRET"]
        )
        self.__ctx = (
            ClientContext(SHAREPOINT_CONFIGS["SITE_URL"])
            .with_credentials(self.__credentials)
        )

    def read_file_content(self, file_url: str, encoding: str = "latin1"):
        complete_url = f'{SHAREPOINT_CONFIGS["BASE_FOLDER_URL"]}/{file_url}'
        response = File.open_binary(self.__ctx, complete_url)
        file_content = response.content

        # Cria um "buffer de arquivo" em memória, decodificado com o encoding informado
        buffer = io.BytesIO(file_content)
        for line in io.TextIOWrapper(buffer, encoding=encoding, errors="replace"):
            yield line.rstrip("\n")

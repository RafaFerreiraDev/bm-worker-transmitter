from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
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
        self.all_file_paths = []

    def explore_folder(self, folder_url: str, relative_path: str ="") -> None:
        folder = self.__ctx.web.get_folder_by_server_relative_url(folder_url)
        folder.expand(["Folders", "Files"]).get().execute_query()

        for file in folder.files:
            full_path = f"{relative_path}{file.properties['Name']}"
            if full_path.endswith((".txt", ".lst")):
                self.all_file_paths.append(full_path)

        for subfolder in folder.folders:
            subfolder_name = subfolder.properties['Name']
            subfolder_url = f"{folder_url}/{subfolder_name}"
            subfolder_relative_path = f"{relative_path}{subfolder_name}/"
            self.explore_folder(subfolder_url, subfolder_relative_path)

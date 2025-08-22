from worker_transmitter_notifier.src.drivers.sharepoint_handler import SharepointHandler
from worker_transmitter_notifier.src.configs import SHAREPOINT_CONFIGS

sharepoint_handler = SharepointHandler()

sharepoint_handler.explore_folder(SHAREPOINT_CONFIGS["BASE_FOLDER_URL"])

print("\n📁 Lista completa de arquivos:\n")
for path in sharepoint_handler.all_file_paths:
    print(path)

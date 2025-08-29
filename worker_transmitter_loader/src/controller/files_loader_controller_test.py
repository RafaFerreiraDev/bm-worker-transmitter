from .files_loader_controller import FilesLoaderController

def test_file_loader_controller():
    files_loader_controller = FilesLoaderController()
    files_loader_controller.load("Saídas/01.2025/SAI_47508411163705_012025.txt")

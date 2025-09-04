from worker_transmitter_xlsx_converter.src.controllers.xlsx_formatter_handler import XlsxFormatterHandler
from worker_transmitter_xlsx_converter.src.controllers.xlsx_filiais_formatter_handler import XlsxMultipleFormatterHandler

XlsxMultipleFormatterHandler().read_s3(
    [
        "GPA/47508411/GERENCIAL_NFE/06.2025 01 a 15.xlsx",
        "GPA/47508411/GERENCIAL_NFE/06.2025 16 a 30.xlsx",
    ],
    2025,
    6
)

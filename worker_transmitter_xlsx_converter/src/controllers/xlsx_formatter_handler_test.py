from worker_transmitter_xlsx_converter.src.controllers.xlsx_formatter_handler import XlsxFormatterHandler


elem = "GPA/47508411/GERENCIAL_NFE/02.2025 16 a 28.xlsx"
XlsxFormatterHandler().read_s3(elem)

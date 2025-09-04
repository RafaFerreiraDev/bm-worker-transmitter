from worker_transmitter_xlsx_converter.src.controllers.xlsx_formatter_handler import XlsxFormatterHandler


elem = "GPA/47508411/GERENCIAL_NFE/01.2025 01 a 15.xlsx"
XlsxFormatterHandler().read_s3(elem)

from .xlsx_formatter_handler import XlsxFormatterHandler

def test_something():
    elem = "GPA/47508411/GERENCIAL_NFE/06.2025 16 a 30.xlsx"
    XlsxFormatterHandler().read_s3(elem)

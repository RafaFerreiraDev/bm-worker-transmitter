import pandas as pd
from worker_transmitter_xlsx_converter.src.drivers.s3_handler import S3Uploader
from .type_map import column_type_map
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io


class XlsxFormatterHandler:
    def __init__(self):
        self.__s3_uploader = S3Uploader()

    def read_s3(self, s3_filename: str):
        excel_stream = self.__s3_uploader.download_fileobj(s3_filename)
        print(excel_stream)

        raw_df = pd.read_excel(excel_stream, engine='openpyxl')
        df = self.__format_col(raw_df)

        table = pa.Table.from_pandas(df, preserve_index=False)

        # Criar buffer Parquet na memória
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer, compression="snappy")

        # Pronto para upload ao S3
        parquet_buffer.seek(0)  # necessário antes de fazer o upload
        self.__s3_uploader.upload_fileobj(parquet_buffer, key="GPA/47508411/DB_PARQUET_SPED_V2/GERENCIAL_NFE/06.2025 16 a 30.parquet")

    def __format_col(self, df: pd.DataFrame):
        cols_remover_zeros = {"Código Produto", "EAN Trib", "EAN"}

        for col in df.columns:
            if col not in column_type_map:
                print(f"Aviso: Coluna não mapeada: {col}")
                continue

            dtype = column_type_map[col]

            try:
                if col in cols_remover_zeros:
                    # Aplica regra de validação: se for número, remove zeros à esquerda
                    df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).isdigit() else str(x))
                    # Depois converte formalmente para string pandas
                    df[col] = df[col].astype("string")

                elif dtype == "string":
                    df[col] = df[col].astype("string")

                elif dtype == "float64":
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype("float64")

                elif dtype == "uint16":
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("UInt16")  # pandas usa "UInt16"

                elif dtype == "date32":
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date  # Mantém apenas a data (sem hora)
                    # opcional: usar dtype="datetime64[ns]" se for salvar em Parquet como datetime

                else:
                    print(f"Tipo não reconhecido para coluna {col}: {dtype}")

            except Exception as e:
                print(f"Erro ao converter coluna {col} para {dtype}: {e}")

        if "CNPJFilial" in df.columns:
            cnpj_col = df["CNPJFilial"]
            vazio = cnpj_col.isna() | (cnpj_col.astype(str).str.strip() == "")

            if vazio.any():
                float_cols = [col for col, dtype in column_type_map.items() if dtype == "float64" and col in df.columns]
                df.loc[vazio, float_cols] = df.loc[vazio, float_cols].fillna(0.0)

        return df

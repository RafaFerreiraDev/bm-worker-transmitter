import pandas as pd
from worker_transmitter_xlsx_converter.src.drivers.s3_handler import S3Uploader
from .type_map import column_type_map
import pyarrow as pa
import pyarrow.parquet as pq
import io


class XlsxMultipleFormatterHandler:
    def __init__(self):
        self.__s3_uploader = S3Uploader()

    def read_s3(self, s3_filenames: list[str], year, month):
        print("Iniciando o processo")
        dfs = []
        for s3_filename in s3_filenames:
            excel_stream = self.__s3_uploader.download_fileobj(s3_filename)
            raw_df = pd.read_excel(excel_stream, engine='openpyxl')
            dfs.append(raw_df)

        # Concatena todos os DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        df = self.__format_col(combined_df)

        # Extrai ano e mês da DataEmissao
        if "DataEmissao" not in df.columns:
            raise ValueError("Coluna 'DataEmissao' não encontrada no arquivo Excel.")

        df["DataEmissao"] = pd.to_datetime(df["DataEmissao"], errors="coerce")
        if df["DataEmissao"].isna().all():
            raise ValueError("Não foi possível converter nenhuma DataEmissao para datetime.")

        #year = df["DataEmissao"].dt.year.iloc[0]
        # month = df["DataEmissao"].dt.month.iloc[0]

        # Cria parquet separado para cada CNPJFilial
        if "CNPJFilial" not in df.columns:
            raise ValueError("Coluna 'CNPJFilial' não encontrada no arquivo Excel.")

        for cnpj, group_df in df.groupby("CNPJFilial"):
            if pd.isna(cnpj) or str(cnpj).strip() == "":
                print("Aviso: Encontrado CNPJFilial vazio, pulando.")
                continue

            table = pa.Table.from_pandas(group_df, preserve_index=False)

            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer, compression="snappy")
            parquet_buffer.seek(0)

            file_name = f"{cnpj}_{year}_0{month}.parquet"
            s3_key = f"GPA/47508411/DB_PARQUET_SPED_V2/GERENCIAL_NFE/FILIAIS/{file_name}"

            print(f"Enviando arquivo {s3_key} para S3...")
            self.__s3_uploader.upload_fileobj(parquet_buffer, key=s3_key)

    def __format_col(self, df: pd.DataFrame):
        cols_remover_zeros = {"Código Produto", "EAN Trib", "EAN"}

        for col in df.columns:
            dtype = column_type_map[col]

            try:
                if col in cols_remover_zeros:
                    # Aplica regra de validação: se for número, remove zeros à esquerda
                    df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).isdigit() else str(x))
                    # Depois converte formalmente para string pandas
                    df[col] = df[col].astype("string")

                elif dtype == "string":
                    if col == "CEST":
                        df[col] = df[col].apply(
                            lambda x: None  # caso seja nulo, vazio ou "0.0"
                            if pd.isna(x) or str(x).strip() in ["", "0.0"]
                            else str(x).split(".")[0] if str(x).endswith(".0") else str(x)
                        )
                        df[col] = df[col].astype("string")
                    else:
                        df[col] = df[col].apply(
                            lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and float(x).is_integer()
                            else str(x) if pd.notna(x) else None
                        )
                        df[col] = df[col].astype("string")

                elif dtype == "float64":
                    if col == "% PIS":
                        # tratamento específico para % PIS
                        df[col] = (
                            df[col]
                            .astype(str)
                            .str.strip()
                            .replace({"": "0", "nan": "0", "None": "0"})   # vazio/nulo vira "0"
                            .str.replace(",", ".", regex=False)            # vírgula -> ponto
                        )
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

                        # garante que nunca vai null no parquet
                        df[col] = df[col].astype("float64").fillna(0.0)

                    else:
                        # regra padrão para outros float64
                        df[col] = (
                            df[col]
                            .astype(str)
                            .str.strip()
                            .replace({"": "0", "nan": "0", "None": "0"})
                            .str.replace(",", ".", regex=False)
                        )
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")

                elif dtype == "uint16":
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("UInt16")  # pandas usa "UInt16"

                elif dtype == "date32":
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date  # Mantém apenas a data (sem hora)
                    # opcional: usar dtype="datetime64[ns]" se for salvar em Parquet como datetime

                else:
                    print(f"Tipo não reconhecido para coluna {col}: {dtype}")

                print(col)
                xxx = df[col][2]
                print(xxx)
                print()

            except Exception as e:
                print(f"Erro ao converter coluna {col} para {dtype}: {e}")

        if "CNPJFilial" in df.columns:
            cnpj_col = df["CNPJFilial"]
            vazio = cnpj_col.isna() | (cnpj_col.astype(str).str.strip() == "")

            if vazio.any():
                float_cols = [col for col, dtype in column_type_map.items() if dtype == "float64" and col in df.columns]
                df.loc[vazio, float_cols] = df.loc[vazio, float_cols].fillna(0.0)

        return df
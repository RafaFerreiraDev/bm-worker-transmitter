from contextlib import closing
import pandas as pd
import io
import boto3
from botocore.exceptions import ClientError
from worker_transmitter_loader.src.configs import S3_CONFIGS


class S3Uploader:
    def __init__(self):
        self.bucket_name = S3_CONFIGS["S3_BUCKET_UPLOAD"]
        self.aws_access_key_id = S3_CONFIGS["AWS_ACCESS_KEY_ID"]
        self.aws_secret_access_key = S3_CONFIGS["AWS_SECRET_ACCESS_KEY"]
        self.region_name = S3_CONFIGS["AWS_DEFAULT_REGION"]
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

    def read_excel_from_s3(self, s3_client, bucket: str, key: str, sheet_name: str = 0) -> pd.DataFrame:
        """
        Reads an Excel (.xlsx or .xls) file from S3 using the shared S3 client.

        :param bucket: S3 bucket name.
        :param key: S3 object key (path to the file).
        :param sheet_name: Sheet name or index (default: first sheet).
        :return: pandas DataFrame containing the Excel data.
        """

        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            buffer = io.BytesIO(content)
            df = pd.read_excel(buffer, sheet_name=sheet_name, engine='calamine', dtype=str)
            print(df.shape)
            return df

        except Exception as e:
            pass

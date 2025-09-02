from contextlib import closing
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

    def download_fileobj(self, key):
        try:
            file_stream = io.BytesIO()
            self.s3_client.download_fileobj(self.bucket_name, key, file_stream)
            file_stream.seek(0)
            return file_stream
        except ClientError as e:
            print(f"Erro ao baixar arquivo: {e}")
            return None

    def upload_fileobj(self, buffer, key):
        buffer.seek(0)
        self.s3_client.upload_fileobj(buffer, self.bucket_name, key)

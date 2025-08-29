from contextlib import closing
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

    def object_exists(self, key: str) -> bool:
        """
        Verifica se o objeto já existe no bucket S3.
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise  # Erro inesperado

    def upload_stream(
        self,
        key: str,
        line_generator,
        encoding: str = "latin1",
        chunk_size: int = 5 * 1024 * 1024,
    ):
        """
        Envia um arquivo para o S3 em streaming usando multipart upload.
        - key: caminho/arquivo dentro do bucket
        - line_generator: generator de linhas (ex: SharepointHandler.read_file_content)
        - encoding: encoding do texto
        - chunk_size: tamanho de cada parte (default 5MB)
        """

        mpu = self.s3_client.create_multipart_upload(Bucket=self.bucket_name, Key=key, Metadata={
            "processo-id": "0198e9c1-743e-73b1-b664-8ff70c9640a1",
            "file-name": key
        })
        upload_id = mpu["UploadId"]

        parts = []
        buffer = bytearray()
        part_number = 1

        try:
            with closing(line_generator):  # garante fechamento do generator, se aplicável
                for line in line_generator:
                    buffer.extend((line + "\n").encode(encoding))
                    if len(buffer) >= chunk_size:
                        response = self.s3_client.upload_part(
                            Bucket=self.bucket_name,
                            Key=key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=bytes(buffer),
                        )
                        parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                        buffer.clear()
                        part_number += 1

                # Envia última parte (se sobrar algo)
                if buffer:
                    response = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=bytes(buffer),
                    )
                    parts.append({"PartNumber": part_number, "ETag": response["ETag"]})

            # Completa upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        except Exception as e:
            # Em caso de erro, cancela upload no S3
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket_name, Key=key, UploadId=upload_id
            )
            raise e

        finally:
            # Libera memória e recursos
            buffer.clear()
            del buffer
            del parts
            del line_generator

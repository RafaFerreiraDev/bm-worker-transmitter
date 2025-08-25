import os

S3_CONFIGS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION"),
    "S3_BUCKET_UPLOAD": os.getenv("S3_BUCKET_UPLOAD"),
    "S3_ROOT_DIR": os.getenv("S3_ROOT_DIR")
}

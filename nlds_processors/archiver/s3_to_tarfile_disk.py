from s3_to_tarfile_stream import S3ToTarfileStream


class S3ToTarfileDisk(S3ToTarfileStream):
    """Class to stream files from / to an S3 resource (AWS, minio, DataCore Swarm etc.)
    to a tarfile that resides on disk.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3"""

    def __init__(self, s3_tenancy: str, s3_access_key: str, s3_secret_key: str) -> None:
        # Initialise the S3 client first
        super().__init__(
            s3_tenancy=s3_tenancy,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
        )

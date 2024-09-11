from s3_to_tarfile_stream import S3ToTarfileStream, S3StreamError

from XRootD import client as XRDClient
from XRootD.client.flags import StatInfoFlags, MkDirFlags, OpenFlags
from typing import Tuple, List
from nlds.details import PathDetails


class S3ToTarfileTape(S3ToTarfileStream):
    """Class to stream files from / to an S3 resource (AWS, minio, DataCore Swarm etc.)
    to a tarfile that resides on tape.

    The streams are defined in these directions:
    PUT : S3 -> Tarfile
    GET : Tarfile -> S3"""

    def __init__(
        self, s3_tenancy: str, s3_access_key: str, s3_secret_key: str, tape_url: str
    ) -> None:
        # Initialise the S3 client first
        super().__init__(
            s3_tenancy=s3_tenancy,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
        )
        # get the location of the tape server and the base directory from the tape_url
        self.tape_server_url, self.tape_base_dir = self._split_tape_url(tape_url)

        # create the connection to the tape server
        self.tape_client = XRDClient.FileSystem(f"root://{self.tape_server_url}")
        self._verify_tape_server()

    def put(self, holding_prefix: str, filelist: List[PathDetails]):
        """
        Put the filelist to the tape server using the already created S3 client and
         tape client.
        The holding_prefix is calculated from the message body in the archive_put or
        archive_get worker process.
        """
        assert self.filelist == []
        self.filelist = filelist
        self.holding_prefix = holding_prefix

        # self._generate_filelist_hash and self._check_files_exist use the member
        # variables already set and the function definitions are in the parent class
        self.filelist_hash = self._generate_filelist_hash()
        self._check_files_exist()

        # Make or find holding folder on the tape server
        status, _ = self.tape_client.mkdir(self.holding_tapepath, MkDirFlags.MAKEPATH)
        if status.status != 0:
            # If bucket directory couldn't be created then fail for retrying
            raise S3StreamError(
                f"Couldn't create or find holding directory ({self.holding_tapepath})."
            )
        
        with XRDClient.File() as XRD_file:
            # Open the file as NEW, to avoid having to prepare it
            flags = OpenFlags.NEW | OpenFlags.MAKEPATH
            status, _ = XRD_file.open(self.tarfile_absolute_tapepath, flags)
            if status.status != 0:
                raise S3StreamError(
                    f"Failed to open file {self.tarfile_absolute_tapepath} for writing"
                )

    """Note that there are a number of different methods below to get the tapepaths"""

    @property
    def holding_tapepath(self):
        """Get the holding tapepath (i.e. the enclosing directory), to be used with
        XRDClient functions on that directory."""
        assert self.tape_base_dir
        assert self.holding_prefix
        return f"{self.tape_base_dir}/{self.holding_prefix}"

    @property
    def tarfile_tapepath(self):
        """Get the tapepath of the tar file, to be used with the XRDClient functions."""
        assert self.tape_base_dir
        assert self.holding_prefix
        assert self.filelist_hash
        return f"{self.tape_base_dir}/{self.holding_prefix}/{self.filelist_hash}.tar"

    @property
    def tarfile_absolute_tapepath(self):
        """Get the absolute tapepath of the tar file, to be used with the XRDClient.
        File functions / constructor, i.e. for the object that is to be streamed to."""
        assert self.tape_base_dir
        assert self.holding_prefix
        assert self.filelist_hash
        assert self.tape_server
        return (
            f"root://{self.tape_server_url}/{self.tape_base_dir}/"
            f"{self.holding_prefix}/{self.filelist_hash}.tar"
        )

    @staticmethod
    def _split_tape_url(tape_url: str) -> Tuple[str]:
        # Verify tape url is valid
        tape_url_parts = tape_url.split("//")
        if not (len(tape_url_parts) == 3 and tape_url_parts[0] == "root:"):
            raise S3StreamError(
                "Tape URL given was invalid. Must be of the "
                "form: root://{server}//{archive/path}, was "
                f"given as {tape_url}."
            )
        _, server, base_dir = tape_url_parts
        # prepend a slash onto the base_dir so it can directly be used to make
        # directories with the pyxrootd client

        return server, f"/{base_dir}"

    def _verify_tape_server(self):
        """Make several simple checks with xrootd to ensure the tape server and
        tape base path, derived form a given tape url, are valid and the xrootd
        endpoint they describe is accessible on the current system.
        """
        # Attempt to ping the tape server to check connection is workable.
        status, _ = self.tape_client.ping()
        if status.status != 0:
            msg = (
                f"Failed status message: {status.message}. ",
                f"Could not ping cta server at {self.tape_server_url}.",
            )
            raise S3StreamError(msg)

        # Stat the base directory and check it's a directory.
        status, resp = self.tape_client.stat(self.tape_base_dir)
        if status.status != 0:
            msg = (
                f"Failed status message: {status.message}. ",
                f"Base dir {self.tape_base_dir} could not be statted",
            )
            raise S3StreamError(msg)
        # Check whether the flag indicates it's a directory
        elif not bool(resp.flags & StatInfoFlags.IS_DIR):
            msg = (
                f"Failed status message: {status.message}. ",
                f"Full status object: {status}. ",
                f"Stat result for base dir {self.tape_base_dir} ",
                f"indicates it is not a directory.",
            )
            raise S3StreamError(msg)

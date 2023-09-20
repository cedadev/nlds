# encoding: utf-8
"""
NOTE: This module is imported into a revision, and so should be very defensive 
with how it imports external modules (like xrootd). 
"""
__author__ = 'Jack Leland and Neil Massey'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2021 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'neil.massey@stfc.ac.uk'

from abc import ABC, abstractmethod
import json
from typing import Tuple, List, Dict, TypeVar
from zlib import adler32

try:
    from pyxrootd.client import File
except ModuleNotFoundError:
    File = TypeVar('File')

from nlds_processors.transferers.base_transfer import (BaseTransferConsumer, 
                                                       TransferError)
from nlds.rabbit.publisher import RabbitMQPublisher as RMQP
from nlds.details import PathDetails


class ArchiveError(Exception):
    pass


class AdlerisingXRDFile():
    """Wrapper class around the XRootD.File object to make it act more like a 
    regular python file object. This means it can interface with packages made 
    for python, e.g. tarfile, BytesIO, minio. This also auto-calculates the 
    adler32 checksum for all written/read bytes from the file, making 
    implentation of checksums within the catalog feasible. 
    """

    def __init__(self, f: File, offset=0, length=0, checksum=1):
        self.f = f
        self.offset = offset
        self.length = length
        self.pointer = 0
        self.checksum = checksum

    def read(self, size):
        """Read some number of bytes (size) from the file, offset by the current 
        pointer position. Note this is wrapped by the adler checksumming but if 
        used within a tarfile read this will not be done purely sequentially so 
        will be essentially meaningless."""
        status, result = self.f.read(offset=self.pointer, size=size)
        if status.status != 0:
            raise IOError(f"Unable to read from file f ({self.f})")
        self.checksum = adler32(result, self.checksum)
        self.pointer += size
        return result
    
    def write(self, b):
        # Update the checksum before we actually do the writing
        self.checksum = adler32(b, self.checksum)
        to_write = len(b)
        self.f.write(b, offset=self.pointer, size=to_write)
        return to_write
    
    def seek(self, whence: int) -> None:
        self.pointer = whence
    
    def tell(self) -> int:
        return self.pointer


class BaseArchiveConsumer(BaseTransferConsumer, ABC):
    DEFAULT_QUEUE_NAME = "archive_q"
    DEFAULT_ROUTING_KEY = (f"{RMQP.RK_ROOT}.{RMQP.RK_ARCHIVE}.{RMQP.RK_WILD}")
    DEFAULT_REROUTING_INFO = f"->{DEFAULT_QUEUE_NAME.upper()}"

    _TAPE_POOL = 'tape_pool'
    _TAPE_URL = 'tape_url'
    _CHUNK_SIZE = 'chunk_size'
    _QUERY_CHECKSUM = 'query_checksum_fl'
    _MAX_RETRIES = 'max_retries'
    _PRINT_TRACEBACKS = 'print_tracebacks_fl'
    ARCHIVE_CONSUMER_CONFIG = {
        _TAPE_POOL: None,
        _TAPE_URL: None,
        _CHUNK_SIZE: 5 * (1024**2), # Default to 5 MiB
        _QUERY_CHECKSUM: True,
        _PRINT_TRACEBACKS: False,
        _MAX_RETRIES: 5,
        RMQP.RETRY_DELAYS: RMQP.DEFAULT_RETRY_DELAYS,
    }
    DEFAULT_CONSUMER_CONFIG = (BaseTransferConsumer.DEFAULT_CONSUMER_CONFIG 
                               | ARCHIVE_CONSUMER_CONFIG)

    def __init__(self, queue=DEFAULT_QUEUE_NAME):
        super().__init__(queue=queue)

        self.tape_pool = self.load_config_value(self._TAPE_POOL)
        self.tape_url = self.load_config_value(self._TAPE_URL)
        self.chunk_size = self.load_config_value(self._CHUNK_SIZE)
        self.query_checksum_fl = self.load_config_value(self._QUERY_CHECKSUM)
        
        # Verify the tape_url is valid, if it exists
        if self.tape_url is not None:
            self.split_tape_url(self.tape_url)

        self.reset()


    def callback(self, ch, method, properties, body, connection):
        """Callback for the base archiver consumer. Takes the message in body 
        runs some standard objectstore verification, taken from the 
        BaseTransferConsumer, as well as some more tape specific config 
        scraping, then runs the appropriate  transfer function. 
        """
        self.reset()
        
        # Convert body from bytes to string for ease of manipulation and then to 
        # a dict 
        body = body.decode("utf-8")
        body_dict = json.loads(body)

        self.log(f"Received {json.dumps(body_dict, indent=4)} from "
                 f"{self.queues[0].name} ({method.routing_key})", 
                 self.RK_LOG_DEBUG)

        # Verify routing key is appropriate
        try:
            rk_parts = self.split_routing_key(method.routing_key)
        except ValueError as e:
            self.log(
                "Routing key inappropriate length, exiting callback.", 
                self.RK_LOG_ERROR
            )
            return

        ### 
        # Verify and load message contents 

        try: 
            transaction_id = body_dict[self.MSG_DETAILS][self.MSG_TRANSACT_ID]
        except KeyError:
            self.log(
                "Transaction id unobtainable, exiting callback.", 
                self.RK_LOG_ERROR
            )
            return

        filelist = self.parse_filelist(body_dict)

        # Set uid and gid from message contents if configured to check 
        # permissions
        if self.check_permissions_fl:
            self.log("Check permissions flag is set, setting uid and gids now.")
            self.set_ids(body_dict)

        try:
            access_key, secret_key, tenancy = self.get_objectstore_config(
                body_dict)
        except TransferError: 
            self.log("Objectstore config unobtainable, exiting callback.", 
                     self.RK_LOG_ERROR)
            return

        try:
            tape_url = self.get_tape_config(body_dict)
        except ArchiveError as e:
            self.log("Tape config unobtainable or invalid, exiting callback.", 
                     self.RK_LOG_ERROR)
            self.log(str(e), self.RK_LOG_DEBUG)
            return

        self.log(f"Starting tape transfer between {tape_url} and object store "
                 f"{tenancy}", self.RK_LOG_INFO)

        # Append route info to message to track the route of the message
        body_dict = self.append_route_info(body_dict)

        self.transfer(transaction_id, tenancy, access_key, secret_key, tape_url, 
                      filelist, rk_parts[0], body_dict)


    def get_tape_config(self, body_dict) -> Tuple:
        """Convenience function to extract tape relevant config from the message
        details section. Currently this is just the tape 
        """
        tape_url = None
        if (self.MSG_TAPE_URL in body_dict[self.MSG_DETAILS] 
                and body_dict[self.MSG_DETAILS][self.MSG_TAPE_URL] is not None):
            tape_url = body_dict[self.MSG_DETAILS][self.MSG_TAPE_URL]
        else:
            tape_url = self.tape_url

        # Check to see whether tape_url has been specified in either the message 
        # or the server_config - exit if not. 
        if tape_url is None:
            self.log(
                "No tenancy specified at server- or request-level, exiting "
                "callback.", 
                self.RK_LOG_ERROR
            )
            raise ArchiveError() 
        
        # Verify the tape_url is valid 
        # NOTE: Might not be necessary as it's checked at startup if using 
        # default or checked at the point of use if used later..
        self.split_tape_url(tape_url)
        
        return tape_url
    

    @staticmethod
    def split_tape_url(tape_url: str) -> Tuple[str]:
        # Verify tape url is valid
        tape_url_parts = tape_url.split("//")
        if not (len(tape_url_parts) == 3 and tape_url_parts[0] == "root:"):
            raise ArchiveError("Tape URL given was invalid. Must be of the "
                               "form: root://{server}//{archive/path}, was "
                               f"given as {tape_url}.")
        _, server, base_dir = tape_url_parts
        # prepend a slash onto the base_dir so it can directly be used to make 
        # directories with the pyxrootd client
        return server, f"/{base_dir}"
    

    @abstractmethod
    def transfer(self, transaction_id: str, tenancy: str, access_key: str, 
                 secret_key: str, tape_url: str, filelist: List[PathDetails], 
                 rk_origin: str, body_dict: Dict[str, str]):
        raise NotImplementedError()
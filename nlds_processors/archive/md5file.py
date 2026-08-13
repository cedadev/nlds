"""
md5file.py
"""

__author__ = "Neil Massey"
__date__ = "30 Apr 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from nlds_processors.archive.checksumfile import ChecksumFile, ChecksumXRDFile
from hashlib import md5
from typing import BinaryIO


def calculate_checksum_md5(fh: BinaryIO) -> int:
    csum = md5()
    while data := fh.read():
        csum.update(data)
    return csum.hexdigest()


class MD5SumMixin:
    def __init__(self, f, checksum=1, debug_fl=False):
        self.f = f
        self.md5sum = md5()
        self.debug_fl = debug_fl

    def update_checksum(self, data):
        self.md5sum.update(data)
        # return the hexdigest, rather than the object
        return self.checksum

    @property
    def checksum(self):
        """Need to return the hexdigest from a MD5 checksum as self.checksum is a
        md5 hashlib object"""
        return self.md5sum.hexdigest()


class MD5File(MD5SumMixin, ChecksumFile):
    """Wrapper class around a File object that auto-calculates the md5 checksum for
    all written/read bytes from the file.
    """


class MD5XRDFile(MD5SumMixin, ChecksumXRDFile):
    """Wrapper class around a XRDFile object to make it act more like a
    regular python file object. This means it can interface with packages made
    for python, e.g. tarfile, BytesIO, minio. This also auto-calculates the
    adler32 checksum for all written/read bytes from the file, making
    implentation of checksums within the catalog feasible.
    """

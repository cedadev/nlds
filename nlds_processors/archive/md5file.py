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


class MD5SumMixin:
    def __init__(self, f, checksum=1, debug_fl=False):
        super.__init__(f, checksum, debug_fl)
        self.md5sum = md5(checksum)

    def update_checksum(self, data):
        self.checksum = self.md5sum.update(data)
        return self.checksum


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

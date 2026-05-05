"""
adler32file.py
"""

__author__ = "Neil Massey"
__date__ = "18 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from zlib import adler32
from nlds_processors.archive.checksumfile import ChecksumFile, ChecksumXRDFile


class Adler32SumMixin:
    def update_checksum(self, data) -> int:
        self.checksum = adler32(data, self.checksum)
        return self.checksum


class Adler32File(Adler32SumMixin, ChecksumFile):
    """Wrapper class around a File object that auto-calculates the adler32 checksum for
    all written/read bytes from the file.
    This implements update_checksum for the ChecksumFile base class, to calculate
    Adler32 checksums.
    """


class Adler32XRDFile(Adler32SumMixin, ChecksumXRDFile):
    """Wrapper class around a XRDFile object to make it act more like a
    regular python file object.
    This implements update_checksum for the ChecksumFile base class, to calculate
    Adler32 checksums.
    """

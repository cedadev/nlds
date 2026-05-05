"""
checksumfile.py
"""

__author__ = "Neil Massey"
__date__ = "30 Apr 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from abc import ABC


class ChecksumFile(ABC):
    """Wrapper class around a File object that auto-calculates the checksum for
    all written/read bytes from the file.
    This is an abstract base class, and a calculate_checksum method must be provided in
    the derived class
    """

    def __init__(self, f, checksum=1, debug_fl=False):
        self.f = f
        self.checksum = checksum
        self.debug_fl = debug_fl

    def read(self, size):
        """Read some number of bytes (size) from the file, offset by the current
        pointer position. Note this is wrapped by the checksumming but if
        used within a tarfile read this will not be done purely sequentially so
        will be essentially meaningless."""
        result = self.f.read(size)
        if len(result) == 0:
            raise IOError(f"Unable to read from file f ({self.f})")
        self.checksum = self.checksum(result, self.checksum)
        return result

    def write(self, b):
        # Update the checksum before we actually do the writing
        self.update_checksum(b)
        to_write = len(b)
        if self.debug_fl:
            print(f"{to_write}")
        status = self.f.write(b)
        if status == 0:
            raise IOError(f"Unable to write to file f {self.f}")
        return to_write

    def seek(self, whence: int) -> None:
        self.f.seek(whence)

    def tell(self) -> int:
        return self.f.tell()

    def update_checksum(self, data):
        raise NotImplementedError


class ChecksumXRDFile:
    """Wrapper class around a XRDFile object to make it act more like a
    regular python file object. This means it can interface with packages made
    for python, e.g. tarfile, BytesIO, minio. This also auto-calculates the
    adler32 checksum for all written/read bytes from the file, making
    implementation of checksums within the catalog feasible.
    This is an abstract base class, and a calculate_checksum method must be provided in
    the derived class
    """

    def __init__(self, f, offset=0, length=0, checksum=1, debug_fl=False):
        self.f = f
        self.offset = offset
        self.length = length
        self.pointer = 0
        self.checksum = checksum
        self.debug_fl = debug_fl

    def read(self, size):
        """Read some number of bytes (size) from the file, offset by the current
        pointer position. Note this is wrapped by the adler checksumming but if
        used within a tarfile read this will not be done purely sequentially so
        will be essentially meaningless."""
        status, result = self.f.read(offset=self.pointer, size=size)
        if status.status != 0:
            raise IOError(f"Unable to read from file f ({self.f})")
        self.update_checksum(result)
        self.pointer += size
        return result

    def write(self, b):
        # Update the checksum before we actually do the writing
        self.update_checksum(b)
        to_write = len(b)
        if self.debug_fl:
            print(f"{self.pointer}:{to_write}")
        status, _ = self.f.write(b, offset=self.pointer, size=to_write)
        if status.status != 0:
            raise IOError(f"Unable to write to file f {self.f}")
        # Move the pointer on
        self.pointer += to_write
        return to_write

    def seek(self, whence: int) -> None:
        self.pointer = whence

    def tell(self) -> int:
        return self.pointer

    def update_checksum(self, data):
        raise NotImplementedError

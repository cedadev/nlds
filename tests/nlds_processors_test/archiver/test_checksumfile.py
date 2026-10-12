"""
test_checksumfile.py
"""

__author__ = "Neil Massey"
__date__ = "30 May 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

"""Test the checksumming classes"""

from nlds_processors.archive.adler32file import Adler32File, Adler32XRDFile
from io import BytesIO


class TestAdler32File:

    def test_write(self):
        f = BytesIO()
        ad32_file = Adler32File(f, debug_fl=True)
        ad32_file.write(bytes("abcdefghijklmnopqrstuvwxz0123456789", encoding="utf-8"))
        assert ad32_file.checksum == 0

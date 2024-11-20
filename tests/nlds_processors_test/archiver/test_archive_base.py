# encoding: utf-8
"""
test_archive_base.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Dec 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import pytest

pyxrootd = pytest.importorskip("pyxrootd")
from nlds_processors.archive.archive_base import ArchiveError, BaseArchiveConsumer


class TestSplitTapeUrl:

    def setup_method(self):
        # Assign the static method split_tape_url as a method on the test class
        self.split_tape_url = BaseArchiveConsumer.split_tape_url

    def test_valid_tape_url(self):
        # Test a valid tape URL
        tape_url = "root://server1//path/to/archive"
        expected_result = ("server1", "/path/to/archive")
        result = self.split_tape_url(tape_url)
        assert result == expected_result

    def test_invalid_tape_url(self):
        # Test an invalid tape URL without the required '//' separator
        tape_url = "root://server2/path/to/archive"
        with pytest.raises(ArchiveError) as exc_info:
            self.split_tape_url(tape_url)
        expected_error_msg = (
            f"Tape URL given was invalid. Must be of the form: "
            f"root://{{server}}//{{archive/path}}, was given as {tape_url}."
        )
        assert str(exc_info.value) == expected_error_msg

    def test_empty_tape_url(self):
        # Test an empty tape URL
        tape_url = ""
        with pytest.raises(ArchiveError) as exc_info:
            self.split_tape_url(tape_url)
        expected_error_msg = (
            f"Tape URL given was invalid. Must be of the form: "
            f"root://{{server}}//{{archive/path}}, was given as {tape_url}."
        )
        assert str(exc_info.value) == expected_error_msg

    def test_missing_server(self):
        # Test a tape URL missing the server component
        tape_url = "root://path/to/archive"
        with pytest.raises(ArchiveError) as exc_info:
            self.split_tape_url(tape_url)
        expected_error_msg = (
            f"Tape URL given was invalid. Must be of the form: "
            f"root://{{server}}//{{archive/path}}, was given as {tape_url}."
        )
        assert str(exc_info.value) == expected_error_msg

    def test_missing_base_dir(self):
        # Test a tape URL missing the base directory
        tape_url = "root://server3//"
        expected_result = ("server3", "/")
        result = self.split_tape_url(tape_url)
        assert result == expected_result

    def test_invalid_prefix(self):
        # Test an invalid tape URL with incorrect prefix
        tape_url = "invalid://server4//path/to/archive"
        with pytest.raises(ArchiveError) as exc_info:
            self.split_tape_url(tape_url)
        expected_error_msg = (
            f"Tape URL given was invalid. Must be of the form: "
            f"root://{{server}}//{{archive/path}}, was given as {tape_url}."
        )
        assert str(exc_info.value) == expected_error_msg

    def test_missing_double_slash(self):
        # Test an invalid tape URL missing the double slash separator
        tape_url = "root:server5//path/to/archive"
        with pytest.raises(ArchiveError) as exc_info:
            self.split_tape_url(tape_url)
        expected_error_msg = (
            f"Tape URL given was invalid. Must be of the form: "
            f"root://{{server}}//{{archive/path}}, was given as {tape_url}."
        )
        assert str(exc_info.value) == expected_error_msg

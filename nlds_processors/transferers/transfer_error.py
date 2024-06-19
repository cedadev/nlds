# encoding: utf-8
"""
transfer_error.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

class TransferError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message

    def __str__(self):
        return self.message
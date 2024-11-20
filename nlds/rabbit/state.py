# encoding: utf-8
"""
state.py
"""

__author__ = "Neil Massey and Jack Leland"
__date__ = "08 Apr 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

# Refactored State into its own file

from enum import Enum


class State(Enum):
    # Generic states
    INITIALISING = -1
    ROUTING = 0
    # PUT workflow states
    SPLITTING = 1
    INDEXING = 2
    CATALOG_PUTTING = 3
    TRANSFER_PUTTING = 4
    # GET workflow states
    CATALOG_GETTING = 10
    ARCHIVE_GETTING = 11
    TRANSFER_GETTING = 12
    TRANSFER_INIT = 13
    # ARCHIVE_PUT workflow states
    ARCHIVE_INIT = 20
    ARCHIVE_PUTTING = 21
    ARCHIVE_PREPARING = 22
    # CATALOG manipulation workflow states
    CATALOG_DELETING = 30
    CATALOG_UPDATING = 31
    CATALOG_ARCHIVE_UPDATING = 32
    CATALOG_REMOVING = 33
    # Complete states
    COMPLETE = 100
    FAILED = 101
    COMPLETE_WITH_ERRORS = 102
    COMPLETE_WITH_WARNINGS = 103
    # Initial state for searching for sub-states
    SEARCHING = 1000

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def has_name(cls, name):
        return name in cls._member_names_

    @classmethod
    def get_final_states(cls):
        final_states = (
            # Make final states explicit
            # cls.TRANSFER_GETTING,
            # cls.CATALOG_UPDATING,
            # cls.CATALOG_ARCHIVE_UPDATING,
            # cls.CATALOG_DELETING,
            # cls.CATALOG_REMOVING,
            cls.FAILED,
            cls.COMPLETE,
        )
        return final_states

    @classmethod
    def get_failed_states(cls):
        return (cls.CATALOG_DELETING, cls.CATALOG_REMOVING, cls.FAILED)

    def to_json(self):
        return self.value

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        raise NotImplementedError

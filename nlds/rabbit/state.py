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
    COMPLETE = 100
    FAILED = 101
    COMPLETE_WITH_ERRORS = 102
    COMPLETE_WITH_WARNINGS = 103
    # PUT workflow states
    SPLITTING = 1
    INDEXING = 2
    CATALOG_PUTTING = 3
    TRANSFER_PUTTING = 4
    CATALOG_ROLLBACK = 5
    CATALOG_UPDATE = 6
    # GET workflow states
    CATALOG_GETTING = 10
    ARCHIVE_GETTING = 11
    TRANSFER_GETTING = 12
    # ARCHIVE_PUT workflow states
    ARCHIVE_INIT = 20
    CATALOG_ARCHIVE_AGGREGATING = 21
    ARCHIVE_PUTTING = 22
    CATALOG_ARCHIVE_UPDATING = 23
    # Shared ARCHIVE states
    CATALOG_ARCHIVE_ROLLBACK = 40
    CATALOG_DELETE_ROLLBACK = 41
    CATALOG_RESTORING = 42
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
            cls.TRANSFER_GETTING,
            #cls.TRANSFER_PUTTING,
            cls.CATALOG_UPDATE,
            cls.CATALOG_ARCHIVE_UPDATING,
            cls.CATALOG_ROLLBACK,
            cls.CATALOG_ARCHIVE_ROLLBACK,
            cls.CATALOG_RESTORING,
            cls.FAILED,
        )
        return final_states

    @classmethod
    def get_failed_states(cls):
        return (cls.CATALOG_ROLLBACK, cls.CATALOG_ARCHIVE_ROLLBACK, cls.FAILED)

    def to_json(self):
        return self.value

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        raise NotImplementedError
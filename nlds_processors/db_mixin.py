# encoding: utf-8
"""
db_mixin.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


class DBError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message


class DBMixin:
    """Mixin refactored from monitor and catalog classes"""

    def get_db_string(self):
        # create the connection string with the engine
        db_connect = self.db_engine_str + "://"
        # add user if defined
        if len(self.db_options["db_user"]) > 0:
            db_connect += self.db_options["db_user"]
            # add password if defined
            if len(self.db_options["db_passwd"]) > 0:
                db_connect += ":" + self.db_options["db_passwd"]
            # add @ symbol
            db_connect += "@"
        # add the database name
        db_connect += self.db_options["db_name"]
        return db_connect

    def connect(self, create_db_fl: bool = True):
        # connect to the database using the information in the config
        # get the database connection string
        db_connect = self.get_db_string()

        # indicate database not connected yet
        self.db_engine = None

        # connect to the database
        try:
            self.db_engine = create_engine(
                db_connect, echo=self.db_options["echo"], future=True
            )
        except ArgumentError as e:
            raise DBError("Could not create database engine")

        # create the db if not already created and flag permits
        if create_db_fl:
            try:
                self.base.metadata.create_all(self.db_engine)
            except IntegrityError as e:
                raise DBError("Could not create database tables")
        # return db_connect string to log
        return db_connect

    def start_session(self):
        """Create a SQL alchemy session"""
        # Check if there is an existing open session
        if self.session is not None:
            # The only way there can be is if there was some error and it wasn't
            # properly finished. We can (probably) just roll it back.
            self.session.rollback()
            self.session = None
        self.session = Session(bind=self.db_engine, future=True)

    def save(self):
        """Commit all pending transactions"""
        self.session.commit()

    def end_session(self):
        """Close the SQL alchemy session"""
        self.session.close()
        self.session = None

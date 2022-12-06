from sqlalchemy.exc import ArgumentError, IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from nlds_processors.catalog.catalog_models import CatalogBase

class DBError(Exception):
    def __init__(self, message, *args):
        super().__init__(args)
        self.message = message

class DBMixin:

    def _get_db_string(self):
        """Mixin refactored from monitor and catalog classes"""
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
    

    def connect(self):
        # connect to the database using the information in the config
        # get the database connection string
        db_connect = self._get_db_string()

        # indicate database not connected yet
        self.db_engine = None

        # connect to the database
        try:
            self.db_engine  = create_engine(
                                db_connect, 
                                echo=self.db_options["echo"]
                            ).connect()
        except ArgumentError as e:
            raise DBError("Could not create database engine")

        # create the db if not already created
        try:
            self.base.metadata.create_all(self.db_engine)
        except IntegrityError as e:
            raise DBError("Could not create database tables")
        # return db_connect string to log
        return db_connect


    def start_session(self):
        """Create a SQL alchemy session"""
        assert(self.session == None)
        self.session = Session(bind=self.db_engine)


    def save(self):
        """Commit all pending transactions"""
        self.session.commit()
        # a commit at the level of the connection to the engine is required
        # when future=True in the create_engine code above
        #self.db_engine.commit()


    def end_session(self):
        """Close the SQL alchemy session"""
        self.session.close()
        self.session = None
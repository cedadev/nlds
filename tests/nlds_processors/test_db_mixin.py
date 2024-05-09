import pytest

from nlds_processors.db_mixin import DBMixin, DBError


class MockDBMixinInheritor(DBMixin):
    """Mock class for testing DBMixin functions in isolation"""

    def __init__(self, db_engine, db_options):
        # Create minimum required attributes to create a working inherited class
        # of DBMixin
        self.db_engine = None
        self.db_engine_str = db_engine
        self.db_options = db_options
        self.sessions = None


class TestCatalogCreation:

    def test_connect(self):
        # Use non-sensical db_engine
        db_engine = "gibberish"
        db_options = {
            "db_name": "/test.db",
            "db_user": "",
            "db_passwd": "",
            "echo": False,
        }
        mock_dbmixin = MockDBMixinInheritor(db_engine, db_options)
        # Should not work as we're trying to use non-sensical db config options
        with pytest.raises(DBError):
            mock_dbmixin.connect()

        # Use non-sensical db_options
        db_engine = "sqlite"
        db_options = list()
        mock_dbmixin = MockDBMixinInheritor(db_engine, db_options)
        # Should not work, but should break when the db_string is made through
        # trying to index like a dictionary and then subsequently calling len()
        with pytest.raises((KeyError, TypeError)):
            mock_dbmixin.connect()

        # What happens if we use an empty dict?
        db_options = dict()
        mock_dbmixin = MockDBMixinInheritor(db_engine, db_options)
        # Should not work when it tries to index things that don't exist
        with pytest.raises(KeyError):
            mock_dbmixin.connect()

        # Try creating a database with functional parameters (no username or
        # password) in memory
        db_engine = "sqlite"
        db_options = {"db_name": "", "db_user": "", "db_passwd": "", "echo": False}
        mock_dbmixin = MockDBMixinInheritor(db_engine, db_options)
        # Should not work as we've not got a Base to create tables from
        with pytest.raises((AttributeError)):
            mock_dbmixin.connect()

        # Try creating a local database with username and password
        db_engine = "sqlite"
        db_options = {
            "db_name": "/test.db",
            "db_user": "test-un",
            "db_passwd": "test-pwd",
            "echo": False,
        }
        mock_dbmixin = MockDBMixinInheritor(db_engine, db_options)
        # Should not work as we can't create a db file with a password or
        # username using pysqlite (the default sqlite engine)
        with pytest.raises((DBError)):
            mock_dbmixin.connect()

        # TODO: Test with a mock SQLAlchemy.Base and see if we can break it in
        # any interesting ways

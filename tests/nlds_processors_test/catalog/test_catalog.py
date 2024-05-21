import uuid
import time

import pytest
from sqlalchemy import func

from nlds_processors.catalog.catalog_models import File, Holding, Transaction
from nlds_processors.catalog.catalog import Catalog, CatalogError
from nlds.details import PathType

test_uuid = "00a246cf-e2a8-46f0-baca-be3972fc4034"


@pytest.fixture()
def mock_catalog():
    # Manually set some settings for a separate test db
    db_engine = "sqlite"
    db_options = {"db_name": "", "db_user": "", "db_passwd": "", "echo": False}
    catalog = Catalog(db_engine, db_options)
    catalog.connect()
    catalog.start_session()
    yield catalog
    catalog.save()
    catalog.end_session()


@pytest.fixture()
def mock_holding():
    return Holding(
        label="test-label",
        user="test-user",
        group="test-group",
    )


@pytest.fixture()
def mock_transaction():
    return Transaction(
        holding_id=None,
        transaction_id=test_uuid,
        ingest_time=func.now(),
    )


@pytest.fixture()
def mock_file():
    new_file = File(
        transaction_id=None,
        original_path="/test/path",
        path_type=PathType["FILE"],
        link_path=None,
        size=1050,
        user="test-user",
        group="test-group",
        file_permissions="0o01577",
    )
    return new_file


@pytest.fixture()
def filled_mock_catalog(mock_catalog, mock_holding, mock_transaction):
    mock_catalog.session.add(mock_holding)
    mock_catalog.session.commit()

    mock_catalog.session.add(mock_transaction)
    mock_catalog.session.commit()

    yield mock_catalog


class TestCatalog:

    def test_get_holding(self, mock_catalog):
        # try on an empty database, should fail
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(user="", group="")
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(user="asgasg", group="assag")

        # First need to add a valid holding to the db
        valid_holding = Holding(
            label="test-label",
            user="test-user",
            group="test-group",
        )
        mock_catalog.session.add(valid_holding)
        mock_catalog.session.commit()

        # Attempt to get the valid holding from the test db
        # Should work with just the user and group
        holding = mock_catalog.get_holding(user="test-user", group="test-group")
        assert len(holding) == 1

        # Should similarly work with correct label or regex search for label
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", label="test-label"
        )
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", label=".*"
        )

        # Try with incorrect information, should fail
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(
                user="incorrect-user", group="test-group"
            )
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(
                user="test-user", group="incorrect-group"
            )
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(
                user="incorrect-user", group="incorrect-group"
            )
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(
                user="test-user", group="test-group", label="incorrect-label"
            )
        # Should also fail with incorrect regex in label
        with pytest.raises(CatalogError):
            holding = mock_catalog.get_holding(
                user="test-user", group="test-group", label="*"
            )

        # We can now add another Holding with the same user and group, but a
        # different label and attempt to get that.
        # First need to add a valid holding to the db
        valid_holding = Holding(
            label="test-label-2",
            user="test-user",
            group="test-group",
        )
        mock_catalog.session.add(valid_holding)
        mock_catalog.session.commit()

        # Should work with correct label
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", label="test-label"
        )
        # NOTE: returns 2 because test-label appears in both labels and this is
        # automatically getting regexed
        assert len(holding) == 2
        # Have to use proper regex to just get 1
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", label="test-label$"
        )
        assert len(holding) == 1

        # And with no label specified?
        holding = mock_catalog.get_holding(user="test-user", group="test-group")
        assert len(holding) == 2

        # And with regex
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", label="test-label"
        )
        assert len(holding) == 2

        # Can try getting by other parameters like holding_id and tag
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", holding_id=1
        )
        assert len(holding) == 1
        holding = mock_catalog.get_holding(
            user="test-user", group="test-group", holding_id=2
        )
        assert len(holding) == 1

        with pytest.raises(CatalogError):
            mock_catalog.get_holding(user="test-user", group="test-group", holding_id=3)
        # TODO: tag logic?

    def test_create_holding(self, mock_catalog):
        # Can attempt to create a holding and then get it
        mock_catalog.create_holding(
            user="test-user", group="test-group", label="test-label"
        )
        # NOTE: Would be wise to directly interface with the database here to
        # make sure the unit-test is isolated?
        holding = (
            mock_catalog.session.query(Holding)
            .filter(
                Holding.user == "test-user",
                Holding.group == "test-group",
                Holding.label == "test-label",
            )
            .all()
        )
        assert len(holding) == 1

        # Attempt to add another identical item, should fail
        with pytest.raises(CatalogError):
            mock_catalog.create_holding(
                user="test-user", group="test-group", label="test-label"
            )
        # need to rollback as the session is otherwise stuck
        mock_catalog.session.rollback()

    def test_modify_holding_with_empty_db(self, mock_catalog):
        # modify_holding requires passing a Holding object to be passed in, so
        # here we'll try with a None instead. Attempting to modify an invalid
        # holding, should result in a CatalogError
        with pytest.raises(CatalogError):
            holding = mock_catalog.modify_holding(None, new_label="new-label")
        # NOTE: the following fail with AttributeErrors as they're not properly
        # input filtering, might be good to be more robust here and make these
        # return catalog errors?
        with pytest.raises(CatalogError):
            holding = mock_catalog.modify_holding(None, new_tags={"key": "val"})
        with pytest.raises(CatalogError):
            holding = mock_catalog.modify_holding(None, del_tags={"key": "val"})

    def test_modify_holding(self, mock_catalog):
        # create a holding to modify
        valid_holding = Holding(
            label="test-label",
            user="test-user",
            group="test-group",
        )
        # Before it's committed to the database, see if we can modify the label
        holding = mock_catalog.modify_holding(valid_holding, new_label="new-label")
        assert holding.label == "new-label"
        # Commit and then attempt to modify again
        mock_catalog.session.add(valid_holding)
        mock_catalog.session.commit()

        holding = mock_catalog.modify_holding(valid_holding, new_label="new-label-2")
        assert holding.label == "new-label-2"
        # Attempting to change or delete the tags of a tagless holding should
        # create new tags
        valid_holding = mock_catalog.modify_holding(
            valid_holding, new_tags={"key": "val"}
        )
        assert len(valid_holding.tags) == 1
        # Get the tags as a dict
        tags = valid_holding.get_tags()
        assert "key" in tags
        assert tags["key"] == "val"

        # Can now modify the existing tag and check it's worked
        valid_holding = mock_catalog.modify_holding(
            valid_holding, new_tags={"key": "newval"}
        )
        assert len(valid_holding.tags) == 1
        # Get the tags as a dict
        tags = valid_holding.get_tags()
        assert "key" in tags
        assert tags["key"] == "newval"

        # Can now attempt to remove the tags.
        # Removing the tag that exists but has a different value should work but
        # not do anything
        valid_holding = mock_catalog.modify_holding(
            valid_holding, del_tags={"key": "val"}
        )
        assert len(valid_holding.tags) == 1
        # Need to commit for changes to take effect
        mock_catalog.session.commit()
        assert len(valid_holding.tags) == 1

        # Removing the actual tag should work
        valid_holding = mock_catalog.modify_holding(
            valid_holding, del_tags={"key": "newval"}
        )
        assert len(valid_holding.tags) == 1
        # Need to commit for changes to take effect
        mock_catalog.session.commit()
        assert len(valid_holding.tags) == 0

        # Deleting a tag that doesn't exist shouldn't work
        with pytest.raises(CatalogError):
            valid_holding = mock_catalog.modify_holding(
                valid_holding, del_tags={"key": "val"}
            )
        assert len(valid_holding.tags) == 0
        # Deleting or modifying without a valid dict should also break. Unlikely
        # to occur in practice and raises a Type error which would be caught by
        # the consumer.
        with pytest.raises(TypeError):
            valid_holding = mock_catalog.modify_holding(
                valid_holding, new_tags="String"
            )
        # TODO: interestingly this raises a CatalogError, as opposed to a
        # TypeError, should probably standardise this.
        with pytest.raises(CatalogError):
            valid_holding = mock_catalog.modify_holding(
                valid_holding, del_tags="String"
            )

        # Finally, attempting to do all of the actions at once should work the
        # same as individually
        holding = mock_catalog.modify_holding(
            valid_holding,
            new_label="new-label-3",
            new_tags={"key-3": "val-3"},
            del_tags={"key-3": "val-3"},
        )
        assert holding.label == "new-label-3"
        # Interestingly, the all-at-once behaviour is different from the one-at-
        # a time behaviour: the tag is deleted without the need for a commit.
        # TODO: should look into this!
        assert len(holding.tags) == 0

    def test_get_transaction(self, mock_catalog):
        test_uuid = str(uuid.uuid4())

        # try on an empty database, should probably fail but currently doesn't
        # because of the one_or_none() on the query in the function.
        transaction = mock_catalog.get_transaction(id=1)
        assert transaction is None
        transaction = mock_catalog.get_transaction(transaction_id=test_uuid)
        assert transaction is None

        # add a transaction to later get
        transaction = Transaction(
            holding_id=2,  # doesn't exist yet
            transaction_id=test_uuid,
        )
        mock_catalog.session.add(transaction)
        mock_catalog.session.commit()

        # Should be able to get by id or transaction_id
        g_transaction = mock_catalog.get_transaction(id=1)
        assert transaction == g_transaction
        assert transaction.id == g_transaction.id
        assert transaction.transaction_id == g_transaction.transaction_id
        g_transaction = mock_catalog.get_transaction(transaction_id=test_uuid)
        assert transaction == g_transaction
        assert transaction.id == g_transaction.id
        assert transaction.transaction_id == g_transaction.transaction_id

        # Try getting a non-existent transaction now we have something in the db
        other_uuid = str(uuid.uuid4)
        g_transaction = mock_catalog.get_transaction(transaction_id=other_uuid)
        assert g_transaction is None

    def test_create_transaction(self, mock_catalog, mock_holding):
        test_uuid = str(uuid.uuid4())
        test_uuid_2 = str(uuid.uuid4())

        # Attempt to create a new transaction, technically requires a Holding.
        # First try with no holding, fails when it tries to reference the
        # holding
        with pytest.raises(AttributeError):
            transaction = mock_catalog.create_transaction(
                holding=None, transaction_id=test_uuid
            )

        # create a holding to create the transaction with
        holding = mock_holding
        mock_catalog.session.add(holding)
        mock_catalog.session.commit()

        # Can now make as many transactions as we want, assuming different uuids
        transaction = mock_catalog.create_transaction(holding, test_uuid)
        transaction_2 = mock_catalog.create_transaction(holding, test_uuid_2)
        assert transaction != transaction_2

        # Using the same uuid should probbaly result in an error but currently
        # doesn't
        # with pytest.raises(CatalogError):
        transaction_3 = mock_catalog.create_transaction(holding, test_uuid)

    def test_user_has_get_holding_permission(self):
        # Leaving this for now until it's a bit more fleshed out
        pass

    def test_user_has_get_file_permission(self):
        # Leaving this for now until it's a bit more fleshed out
        pass

    def test_get_files(self, mock_catalog, mock_holding, mock_transaction, mock_file):
        test_uuid = str(uuid.uuid4())
        catalog = mock_catalog

        # Getting shouldn't work on an empty database
        with pytest.raises(CatalogError):
            files = catalog.get_files("test-user", "test-group")
        # Try with garbage input
        with pytest.raises(CatalogError):
            files = catalog.get_files("ihasidg", "oihaosifh")
        # Try with reasonable values in all optional kwargs
        with pytest.raises(CatalogError):
            files = catalog.get_files(
                "test-user",
                "test-group",
                holding_label="test-label",
                holding_id=1,
                transaction_id=test_uuid,
                original_path="/test/path",
                tag={"key": "val"},
            )
        # Try with garbage in all optional kwargs
        with pytest.raises(CatalogError):
            files = catalog.get_files(
                "asgad",
                "agdasd",
                holding_label="ououg",
                holding_id="asfasf",
                transaction_id="adgouihoih",
                original_path="oihosidhag",
                tag={"aegaa": "as"},
            )

        # Add mock holding and transaction to db so ids are populated.
        catalog.session.add(mock_holding)
        catalog.session.flush()
        mock_transaction.holding_id = mock_holding.id
        catalog.session.add(mock_transaction)
        catalog.session.flush()

        # Getting still shouldn't work on the now initialised database
        with pytest.raises(CatalogError):
            files = catalog.get_files("test-user", "test-group")
        # Try with garbage input
        with pytest.raises(CatalogError):
            files = catalog.get_files("ihasidg", "oihaosifh")
        # Try with reasonable values in all optional kwargs
        with pytest.raises(CatalogError):
            files = catalog.get_files(
                "test-user",
                "test-group",
                holding_label="test-label",
                holding_id=1,
                transaction_id=test_uuid,
                original_path="/test/path",
                tag={"key": "val"},
            )
        # Try with garbage in all optional kwargs
        with pytest.raises(CatalogError):
            files = catalog.get_files(
                "asgad",
                "agdasd",
                holding_label="ououg",
                holding_id="asfasf",
                transaction_id="adgouihoih",
                original_path="oihosidhag",
                tag={"aegaa": "as"},
            )

        # Make a file for us to get
        new_file = mock_file
        new_file.transaction_id = mock_transaction.id
        catalog.session.add(new_file)
        catalog.session.commit()

        files = catalog.get_files("test-user", "test-group")
        assert isinstance(files, list)
        assert len(files) == 1
        assert files[0] == new_file

        # Add some more files with the same user
        for i in range(10):
            new_file_2 = File(
                transaction_id=mock_transaction.id,
                original_path=f"/test/path-{i}",
                path_type=PathType["FILE"],
                link_path=None,
                size=1050,
                user="test-user",
                group="test-group",
                file_permissions="0o01577",
            )
            catalog.session.add(new_file_2)
            catalog.session.commit()
            files = catalog.get_files("test-user", "test-group")
            assert isinstance(files, list)
            assert len(files) == i + 2

    def test_create_file(self):
        pass

    def test_delete_files(self):
        pass

    def test_get_location(self):
        pass

    def test_create_location(self):
        pass

    def test_create_tag(self):
        pass

    def test_get_tag(self):
        pass

    def test_modify_tag(self):
        pass

    def test_delete_tag(self):
        pass

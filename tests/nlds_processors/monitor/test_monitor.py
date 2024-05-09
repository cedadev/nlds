import pytest

from nlds_processors.monitor.monitor_models import (
    MonitorBase,
    TransactionRecord,
    SubRecord,
    FailedFile,
    Warning,
)
from nlds_processors.monitor.monitor import Monitor, MonitorError


@pytest.fixture()
def mock_monitor():
    # Manually set some settings for test db in memory, very basic.
    db_engine = "sqlite"
    db_options = {"db_name": "", "db_user": "", "db_passwd": "", "echo": False}
    # Set up
    monitor = Monitor(db_engine, db_options)
    monitor.connect()
    monitor.start_session()

    # Provide to method
    yield monitor

    # Tear down
    monitor.save()
    monitor.end_session()


def test_create_transaction_record(mock_monitor):
    pass


def test_get_transaction_record(mock_monitor):
    pass


def test_create_sub_record(mock_monitor):
    pass


def test_get_sub_record(mock_monitor):
    pass


def test_get_sub_records(mock_monitor):
    pass


def test_update_sub_record(mock_monitor):
    pass


def test_create_failed_file(mock_monitor):
    pass


def test_check_completion(mock_monitor):
    pass


def test_create_warning(mock_monitor):
    pass

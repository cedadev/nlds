# encoding: utf-8
"""
test_logger.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import copy
import json
import pytest
import functools
import logging

from nlds.rabbit import publisher as publ
from nlds_processors.logger import LoggingConsumer

import nlds.rabbit.message_keys as MSG
import nlds.server_config as CFG


def mock_load_config(template_config):
    return template_config


class MockLogger(LoggingConsumer):
    def callback(self, ch, method, properties, body, connection):
        pass


@pytest.fixture(scope="session")
def debug_root_logger():
    # Set root logging level to debug for the session
    logging.root.setLevel(logging.DEBUG)


@pytest.fixture()
def default_logger(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(
        CFG, "load_config", functools.partial(mock_load_config, template_config)
    )
    return LoggingConsumer()


def assert_last_caplog(caplog, log_level="ERROR", clear_fl=False):
    # Check that the last logrecord is a message of log_level
    assert caplog.records[-1].levelname == log_level
    if clear_fl:
        caplog.clear()


@pytest.mark.parametrize("rk_log_level", LoggingConsumer._logging_levels)
def test_callback(
    debug_root_logger,
    caplog,
    default_logger,
    default_rmq_method,
    default_rmq_logmsg_dict,
    rk_log_level,
):
    # Let caplog capture all log messages
    caplog.set_level(logging.DEBUG)

    test_message = "message"

    # Add necessary logging data to message dict and convert into
    # callback-friendly bytes. Note we're using general nlds logger here
    default_rmq_logmsg_dict[MSG.DETAILS][MSG.LOG_TARGET] = "nlds.root"
    default_rmq_logmsg_dict[MSG.DATA][MSG.LOG_MESSAGE] = test_message
    routed_body = json.dumps(default_rmq_logmsg_dict)

    # Attempt to run callback with default message, should complete, but raise
    # an error in the logs due to a faulty routing_key
    default_logger.callback(None, default_rmq_method, None, routed_body, None)
    assert_last_caplog(caplog, clear_fl=True)

    # TODO (2022-08-02): Note that caplog doesn't seem to work with debug
    # messages at the moment, so the debug message will fail. Still fails at
    # newest version of pytest (7.1.2) so we will need to revisit when a
    # solution to this issue https://github.com/pytest-dev/pytest/issues/7335 is
    # found. Also worth noting that we know from integration testing that debug
    # messages work so this is not a major risk.
    if rk_log_level == "debug":
        return

    # Attempt to run with a known, sensible routing key - should not fail but we
    # need to check the log contents afterwards
    custom_method = copy.deepcopy(default_rmq_method)
    custom_method.routing_key = f"nlds.test.{rk_log_level}"
    default_logger.callback(None, custom_method, None, routed_body, None)
    assert len(caplog.records) == 3

    # Check that the proper message type was logged
    # The final should be a confirmation info message
    assert caplog.records[-1].message == f"Callback finished. \n"
    assert caplog.records[-1].levelname == "INFO"
    # The penultimate should be the type asked for in the message to the logger
    assert caplog.records[-2].message == test_message
    assert caplog.records[-2].levelname == rk_log_level.upper()

    # Check it breaks with a non-sensible routing key
    custom_method.routing_key = "nlds.test.non-sensible"
    default_logger.callback(None, custom_method, None, routed_body, None)
    assert_last_caplog(caplog)

    # Check it breaks with a non-sensible log target
    custom_method.routing_key = f"nlds.test.info"
    default_rmq_logmsg_dict[MSG.DETAILS][MSG.LOG_TARGET] = "test"
    routed_body = json.dumps(default_rmq_logmsg_dict)
    default_logger.callback(None, custom_method, None, routed_body, None)

    # Last message should be as stated and written at error level
    assert "Invalid log target provided" in caplog.records[-1].message
    assert_last_caplog(caplog)

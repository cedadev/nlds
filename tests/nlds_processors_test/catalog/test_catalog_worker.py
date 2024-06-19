# encoding: utf-8
"""
test_catalog_worker.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "07 Dec 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import pytest
import functools

from nlds.rabbit import publisher as publ
from nlds_processors.catalog.catalog_worker import CatalogConsumer


def mock_load_config(template_config):
    return template_config


@pytest.fixture()
def default_catalog(monkeypatch, template_config):
    # Ensure template is loaded instead of .server_config
    monkeypatch.setattr(
        publ, "load_config", functools.partial(mock_load_config, template_config)
    )
    return CatalogConsumer()

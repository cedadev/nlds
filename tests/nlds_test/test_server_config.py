# encoding: utf-8
"""
test_server_config.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "19 Jun 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import json
import os
import copy
from typing import Iterable

import pytest

# from fixtures import template_config, TEMPLATE_CONFIG_PATH
from nlds.server_config import (
    load_config,
    validate_config_file,
    RABBIT_CONFIG_SECTION,
    AUTH_CONFIG_SECTION,
    CONFIG_SCHEMA,
)


TEMPLATE_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "../../nlds/templates/server_config.j2"
)


def test_load_config():
    # Check that the template can be effectively loaded, as expected
    assert isinstance(load_config(TEMPLATE_CONFIG_PATH), dict)

    # Check that appropriate errors are raised if erroneous paths are given.
    with pytest.raises(IsADirectoryError):
        load_config("/")
    with pytest.raises(FileNotFoundError):
        load_config("/.server_config")


def test_validate_config_file(template_config):
    # Make a copy of the template config so we can edit it
    json_config = copy.deepcopy(template_config)
    assert isinstance(json_config, dict)

    # Check schema is not broken
    schema_dict = dict(CONFIG_SCHEMA)
    for section_heading, section_labels in schema_dict.items():
        assert section_heading in json_config
        assert isinstance(section_labels, Iterable)

    # Check template passes validation unaltered
    validate_config_file(json_config)

    with pytest.raises(RuntimeError):
        # Try validating only one section at a time

        validate_config_file(json_config[AUTH_CONFIG_SECTION])
        validate_config_file(json_config[RABBIT_CONFIG_SECTION])

        json_config.pop(AUTH_CONFIG_SECTION)
        validate_config_file(json_config)
        json_config.pop(RABBIT_CONFIG_SECTION)
        validate_config_file(json_config)

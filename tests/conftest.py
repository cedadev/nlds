import os
import json
from uuid import UUID

import pytest


TEMPLATE_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 
                                    '../nlds/templates/server_config.j2')

@pytest.fixture
def template_config():
    config_path = TEMPLATE_CONFIG_PATH
    fh = open(config_path)
    return json.load(fh)

@pytest.fixture
def test_uuid():
    return UUID("3fa85f64-5717-4562-b3fc-2c963f66afa6")

@pytest.fixture
def edge_values():
    return ("", " ", ".", None, "None")
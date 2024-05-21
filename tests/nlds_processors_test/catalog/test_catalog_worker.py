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

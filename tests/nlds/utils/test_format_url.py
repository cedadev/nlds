from nlds.utils.format_url import format_url


def test_no_parts():
    assert format_url([]) == ""


def test_single_part():
    assert format_url(["http://example.com"]) == "http://example.com"


def test_multiple_parts():
    url_parts = ["http://example.com", "path", "to", "resource"]
    expected_url = "http://example.com/path/to/resource"
    assert format_url(url_parts) == expected_url


def test_with_query_params():
    url_parts = ["http://example.com", "path", "to", "resource"]
    query_params = {"key1": "value1", "key2": "value2"}
    expected_url = "http://example.com/path/to/resource?key1=value1&key2=value2"
    assert format_url(url_parts, query_params) == expected_url


def test_empty_query_params():
    url_parts = ["http://example.com", "path", "to", "resource"]
    query_params = {}
    expected_url = "http://example.com/path/to/resource"
    assert format_url(url_parts, query_params) == expected_url


def test_complex_query_params():
    url_parts = ["http://example.com", "search"]
    query_params = {"q": "test search", "page": "1", "sort": "asc"}
    expected_url = "http://example.com/search?q=test+search&page=1&sort=asc"
    assert format_url(url_parts, query_params) == expected_url

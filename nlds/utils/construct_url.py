from urllib.parse import urljoin, urlencode

def construct_url(url_parts, query_params=None):
    """
    Constructs a URL from a list of parts.
    
    Parameters:
    url_parts (list): A list of URL parts to be joined.
    query_params (dict, optional): A dictionary of query parameters to be appended to the URL.

    Returns:
    base (str): The constructed URL.
    """
    if not url_parts:
        return ""
    
    # Start with the base part
    base = url_parts[0]

    # Join the rest of the parts to the base URL
    for part in url_parts[1:]:
        base = urljoin(base + '/', part)

    # Add query parameters if provided
    if query_params:
        base += '?' + urlencode(query_params)

    return base
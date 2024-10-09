# encoding: utf-8
"""
is_regex.py
"""
__author__ = "Neil Massey"
__date__ = "03 Oct 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

def is_regex(input: str) -> bool:
    """Check whether the input string contains regular expressions."""
    regex_matches = (
        '[', ']', '{', '}', '^', '|', '\s', '\S', '\d', '\D', '\w', '\W',
        '(:?', '(', ')', '?', '*', '+', '$', '\b', '\B'
    )
    for r in regex_matches:
        if r in input:
            return True
    return False
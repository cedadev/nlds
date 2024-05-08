"""
delays.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "08 Apr 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from datetime import timedelta

# In ascending order: 0 seconds, 30 seconds, 1 minute, 1 hour, 1 day, 5 days
# All must be in milliseconds.
RETRY_DELAYS = "retry_delays"
DEFAULT_RETRY_DELAYS = [
    timedelta(seconds=0).total_seconds() * 1000,
    timedelta(seconds=30).total_seconds() * 1000,
    timedelta(minutes=1).total_seconds() * 1000,
    timedelta(hours=1).total_seconds() * 1000,
    timedelta(days=1).total_seconds() * 1000,
    timedelta(days=5).total_seconds() * 1000,
]
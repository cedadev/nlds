# encoding: utf-8
"""
nlds_setup.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"


API_VERSION = "0.1"
CONFIG_FILE_LOCATION = "/etc/nlds/server_config"
USE_DISKTAPE = False
if USE_DISKTAPE:
    DISKTAPE_LOC = "~/DISKTAPE"
else:
    DISKTAPE_LOC = None
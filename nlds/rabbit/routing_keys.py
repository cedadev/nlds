# encoding: utf-8
"""
routing_keys.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "08 Apr 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

# Refactored routing keys into their own file

# Routing key constants
PUT = "put"
GET = "get"
DEL = "del"
PUTLIST = "putlist"
GETLIST = "getlist"
DELLIST = "dellist"
LIST = "list"
STAT = "stat"
FIND = "find"
META = "meta"
SYSTEM_STAT = "system-stat"

# Exchange routing key parts – root
ROOT = "nlds-api"
WILD = "*"

# Exchange routing key parts – queues
INDEX = "index"
CATALOG = "catalog"
CATALOG_PUT = "catalog-put"
CATALOG_GET = "catalog-get"
CATALOG_DEL = "catalog-del"
CATALOG_UPDATE = "catalog-update"
CATALOG_REMOVE = "catalog-remove"
MONITOR = "monitor"
MONITOR_PUT = "monitor-put"
MONITOR_GET = "monitor-get"
TRANSFER = "transfer"
TRANSFER_PUT = "transfer-put"
TRANSFER_GET = "transfer-get"
ARCHIVE = "archive"
ARCHIVE_PUT = "archive-put"
ARCHIVE_GET = "archive-get"
ARCHIVE_RESTORE = "archive-restore"
CATALOG_ARCHIVE_NEXT = "catalog-archive-next"
CATALOG_ARCHIVE_UPDATE = "catalog-archive-update"
ROUTE = "route"
LOG = "log"

# Exchange routing key parts – actions
INITIATE = "init"
START = "start"
COMPLETE = "complete"
FAILED = "failed"
NEXT = "next"
PREPARE = "prepare"
PREPARE_CHECK = "prepare-check"

# Exchange routing key parts – monitoring levels
LOG_NONE = "none"
LOG_DEBUG = "debug"
LOG_INFO = "info"
LOG_WARNING = "warning"
LOG_ERROR = "error"
LOG_CRITICAL = "critical"
LOG_RKS = (
    LOG_NONE,
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARNING,
    LOG_ERROR,
    LOG_CRITICAL,
)
LOGGER_PREFIX = "nlds."
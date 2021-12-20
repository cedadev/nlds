# API Commands

# NRM - Are these constants used in more than file?  If not, we might not need
# this as an import.  We could just declare them in the file they are used in.

GET = "GET"
GETLIST = "GETLIST"
PUT = "PUT"
PUTLIST = "PUTLIST"
POST = "POST"
DELETE = "DELETE"

# Config section strings
AUTH_CONFIG_SECTION = "authentication"
RABBIT_CONFIG_SECTION = "rabbitMQ"
RABBIT_CONFIG_EXCHANGES = "exchanges"
RABBIT_CONFIG_QUEUES = "queues"
RABBIT_CONFIG_QUEUE_NAME = "name"
CONFIG_SCHEMA = (
    (AUTH_CONFIG_SECTION, ("authenticator_backend", )),
    (RABBIT_CONFIG_SECTION, ("user", "password", "server", "vhost", "exchange", "queues")),
)

# Message labels
DETAILS = "details"
TRANSACT_ID = "transaction_id"
TIMESTAMP = "timestamp"
USER = "user"
GROUP = "group"
TARGET = "target"
DATA = "data"
DATA_FILELIST = "filelist"
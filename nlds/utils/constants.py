GET = "GET"
GETLIST = "GETLIST"
PUT = "PUT"
PUTLIST = "PUTLIST"
POST = "POST"
DELETE = "DELETE"

AUTH_CONFIG_SECTION = "authentication"
RABBIT_CONFIG_SECTION = "rabbitMQ"
RABBIT_CONFIG_EXCHANGES = "exchanges"
RABBIT_CONFIG_QUEUES = "queues"
CONFIG_SCHEMA = (
    (AUTH_CONFIG_SECTION, ("authenticator_backend", )),
    (RABBIT_CONFIG_SECTION, ("user", "password", "server", "vhost", "exchange")),
)
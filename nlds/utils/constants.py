# Config file section strings
AUTH_CONFIG_SECTION = "authentication"
RABBIT_CONFIG_SECTION = "rabbitMQ"
RABBIT_CONFIG_EXCHANGES = "exchanges"
RABBIT_CONFIG_QUEUES = "queues"
RABBIT_CONFIG_QUEUE_NAME = "name"
CONFIG_SCHEMA = (
    (AUTH_CONFIG_SECTION, ("authenticator_backend", )),
    (RABBIT_CONFIG_SECTION, ("user", "password", "server", "vhost", "exchange", "queues"))
)
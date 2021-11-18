GET = "GET"
GETLIST = "GETLIST"
PUT = "PUT"
POST = "POST"
DELETE = "DELETE"

AUTH_CONFIG_SECITON = "authentication"
RABBIT_CONFIG_SECTION = "rabbitMQ"
CONFIG_SCHEMA = (
    (AUTH_CONFIG_SECITON, ("authenticator_backend", )),
    (RABBIT_CONFIG_SECTION, ("user", "password", "server", "vhost", "exchange")),
)
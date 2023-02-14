Server config
=============

The server config file controls the configurable behaviour of the NLDS. It is a 
json file split into dictionary sections, with each section delineating 
configuration for a specific part of the program. There is an example 
server_config in the templates section of the main nlds package 
(``nlds.templates.server_config``) to get you started, but this page will 
demystify the configuration needed for (a) a local development copy of the nlds, 
and (b) a production system spread across several pods/virtual machines. 

*Please note that the NLDS is in active development and all of this is subject 
to change with no notice.*

Required sections
-----------------

There are two required sections for every server_config: ``authentication`` and 
``rabbitMQ``.

Authentication
^^^^^^^^^^^^^^
This deals with how users are authenticated through the OAuth2 flow used in the 
client. The following fields are required in the dictionary::

    "authentication" : {
        "authenticator_backend" : "jasmin_authenticator",
        "jasmin_authenticator" : {
            "user_profile_url" : "{{ user_profile_url }}",
            "user_services_url" : "{{ user_services_url }}",
            "oauth_token_introspect_url" : "{{ token_introspect_url }}"
        }
    }

where ``authenticator_backend`` dictates which form of authentication you would 
like to use. Currently the only implemented authenticator is the 
``jasmin_authenticator``, but there are plans to expand this to also work with 
other industry standard authenticators like google and microsoft. 

The authenticator setup is then specified in a separate dictionary named after 
the authenticator, which is specific to each authenticator. The 
``jasmin_authenticator`` requires, as above, values for ``user_profile_url``, 
``user_services_url``, and ``oauth_token_introspect_url``. This cannot be 
divulged publicly on github for JASMIN, so please get in contact for the actual 
values to use. 

RabbitMQ
^^^^^^^^

This deals with how the nlds connects to the RabbitMQ queue and message 
brokering system. The following is an outline of what is required::

    "rabbitMQ": {
        "user": "{{ rabbit_user }}",
        "password": "{{ rabbit_password }}",
        "server": "{{ rabbit_server }}",
        "vhost": "{{ rabbit_vhost }}",
        "exchange": {
            "name": "{{ rabbit_exchange_name }}",
            "type": "{{ rabbit_exchange_type }}",
            "delayed": "{{ rabbit_exchange_delayed }}"
        },
        "queues": [
            {
                "name": "{{ rabbit_queue_name }}",
                "bindings": [
                    {
                        "exchange": "{{ rabbit_exchange_name }}",
                        "routing_key": "{{ rabbit_queue_routing_key }}"
                    }
                ]
            }
        ]
    }

Here the ``user`` and ``password`` fields refer to the username and password for 
the rabbit server you wish to connect to, which is in turn specified with 
``server``. ``vhost`` is similarly the virtual host on the rabbit server that 
you wish to connect to. 

The next two dictionaries are context specific. All publishing elements of the 
NLDS, i.e. parts that will send messages, will require an exchange to publish 
messages to. ``exchange`` is determines that exchange, with three required 
subfields: ``name``, ``type``, and ``delayed``. The former two are self 
descriptive, they should just be the name of the exchange on the virtualhost and 
it's corresponding type e.g. one of fanout, direct or topic. ``delay`` is a 
boolean (``true`` or ``false`` in json-speak) dictating whether to use the 
delay functionality utilised within the NLDS. Note that this requires the rabbit 
server have the DelayedRabbitExchange plugin installed.

Exchanges can be declared and created if not present on the virtual host the 
first time the NLDS is run, virtualhosts cannot and so will have to be created 
beforehand manually on the server or through the admin interface. If an exchange 
is requested but incorrect information given about either its `type` or 
`delayed` status, then the NLDS will throw an error. 

``queues`` is a list of queue dictionaries and must be implemented on consumers, 
i.e. message processors, to tell ``pika`` where to take messages from. Each 
queue dictionary consists of a ``name`` and a list of `bindings`, with each 
``binding`` being a dictionary containing the name of the ``exchange`` the queue 
takes messages from, and the routing key that a message must have to be accepted 
onto the queue. For more information on exchanges, routing keys, and other 
RabbitMQ features, please see [Rabbit's excellent documentation]
(https://www.rabbitmq.com/tutorials/tutorial-five-python.html). 


Generic optional sections
-------------------------

There are 2 generic sections, i.e. those which are used across the NLDS 
ecosystem, but are optional and therefore fall back on a default configuration 
if not specified. These are ``logging``, and ``general``.  

Logging
^^^^^^^

General
^^^^^^^

Consumer-specific optional sections
-----------------------------------

NLDS Worker
^^^^^^^^^^^

Indexer
^^^^^^^

Cataloguer
^^^^^^^^^^

Transfer-put
^^^^^^^^^^^^

Transfer-get
^^^^^^^^^^^^

Monitor
^^^^^^^

Logger
^^^^^^

Examples
========

Local NLDS
----------

Distributed NLDS
----------------
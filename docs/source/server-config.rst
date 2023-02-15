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

The logging configuration options look like the following::

    "logging": {
        "enable": boolean
        "log_level": str  - ("none" | "debug" | "info" | "warning" | "error" | "critical"),
        "log_format": str - see python logging docs for details,
        "add_stdout_fl": boolean,
        "stdout_log_level": str  - ("none" | "debug" | "info" | "warning" | "error" | "critical"),
        "rollover": str - see python logging docs for details
    }

These all set default options the native python logging system, with 
``log_level`` being the log level, ``log_format`` being a string describing the 
log output format, and rollover describing the frequency of rollover for log 
files in the standard manner. For details on all of this, see the python docs 
for inbuilt logging. ``enable`` and ``add_stdout_fl`` are boolean flags 
controlling log output to files and ``stdout`` respectively, and the 
``stdout_log_level`` is the log level for the stdout logging, if you require it 
to be different from the default log level. 

As stated, these all set the default log options for all publishers and 
consumers within the NLDS - these can be overridden on a consumer-specific basis 
by inserting a ``logging`` sub-dictionary into a consumer-specific optional 
section.

General
^^^^^^^

The general config, as of writing this page, only covers one option: the 
retry_delays list::

    "general": {
        "retry_delays": List[int]
    }

This retry delays list gives the delay applied to retried messages in seconds, 
with the `n`th element being the delay for the `n`th retry. Setting the value 
here sets a default for _all_ consumers, but the retry_delays option can be 
inserted into any consumer-specific config to override this. 

Consumer-specific optional sections
-----------------------------------

Each of the consumers have their own configuration dictionary, named by 
convention as ``{consumername}_q``, e.g. ``transfer_put_q``. Each has a set of 
default options and will accept both a logging dictionary and a retry_delays 
list for consumer-specific override of the default options, mentioned above. 
Each consumer also has a specific set of config options, some shared, which will 
control its behaviour. The following is a brief rundown of the server config 
options for each consumer. 

NLDS Worker
^^^^^^^^^^^
The server config section is ``nlds_q``, and the following options are available::

    "nlds_q":{
        "logging": [standard_logging_dictionary],
        "retry_delays": List[int]
        "print_tracebacks_fl": boolean,
    }

Not much specifically happens in the NLDS worker that requires configuration, so 
it basically just has the default settings. One that has not been covered yet, 
``print_tracebacks_fl``, is a boolean flag to control whether the full 
stacktrace of any caught exception is sent to the logger. This is a standard 
across all consumers. You may set retry_delays if you wish but the NLDS worker 
doesn't retry messages specifically, only in the case of something going 
unexpectedly wrong.

Indexer
^^^^^^^

Server config section is ``index_q``, and the following options are available::

    "index_q":{
        "logging": {standard_logging_dictionary},
        "retry_delays": List[int]
        "print_tracebacks_fl": boolean,
        "filelist_max_length": int,
        "message_threshold": int,
        "max_retries": int,
        "check_permissions_fl": boolean,
        "check_filesize_fl": boolean,
    }

where ``logging``, ``retry_delays``, and ``print_tracebacks_fl`` are as above.

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
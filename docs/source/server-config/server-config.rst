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
``server``. ``vhost`` is similarly the virtualhost on the rabbit server that 
you wish to connect to. 

The next two dictionaries are context specific. All publishing elements of the 
NLDS, i.e. parts that will send messages, will require an exchange to publish 
messages to. ``exchange`` is determines that exchange, with three required 
subfields: ``name``, ``type``, and ``delayed``. The former two are self 
descriptive, they should just be the name of the exchange on the `virtualhost` and 
it's corresponding type e.g. one of fanout, direct or topic. ``delay`` is a 
boolean (``true`` or ``false`` in json-speak) dictating whether to use the 
delay functionality utilised within the NLDS. Note that this requires the rabbit 
server have the DelayedRabbitExchange plugin installed.

Exchanges can be declared and created if not present on the `virtualhost` the 
first time the NLDS is run, `virtualhosts` cannot and so will have to be created 
beforehand manually on the server or through the admin interface. If an exchange 
is requested but incorrect information given about either its `type` or 
`delayed` status, then the NLDS will throw an error. 

``queues`` is a list of queue dictionaries and must be implemented on consumers, 
i.e. message processors, to tell ``pika`` where to take messages from. Each 
queue dictionary consists of a ``name`` and a list of ``bindings``, with each 
``binding`` being a dictionary containing the name of the ``exchange`` the queue 
takes messages from, and the routing key that a message must have to be accepted 
onto the queue. For more information on exchanges, routing keys, and other 
RabbitMQ features, please see `Rabbit's excellent documentation <https://www.rabbitmq.com/tutorials/tutorial-five-python.html>`_. 


Generic optional sections
-------------------------

There are 2 generic sections, i.e. those which are used across the NLDS 
ecosystem, but are optional and therefore fall back on a default configuration 
if not specified. These are ``logging``, and ``general``.  

Logging
^^^^^^^

The logging configuration options look like the following::

    "logging": {
        "enable": boolean,
        "log_level": str  - ("none" | "debug" | "info" | "warning" | "error" | "critical"),
        "log_format": str - see python logging docs for details,
        "add_stdout_fl": boolean,
        "stdout_log_level": str  - ("none" | "debug" | "info" | "warning" | "error" | "critical"),
        "log_files": List[str],
        "max_bytes": int,
        "backup_count": int
    }

These all set default options the native python logging system, with 
``log_level`` being the log level, ``log_format`` being a string describing the 
log output format, and rollover describing the frequency of rollover for log 
files in the standard manner. For details on all of this, see the python docs 
for inbuilt logging. ``enable`` and ``add_stdout_fl`` are boolean flags 
controlling log output to files and ``stdout`` respectively, and the 
``stdout_log_level`` is the log level for the stdout logging, if you require it 
to be different from the default log level. 

``log_files`` is a list of strings describing the path or paths to log files 
being written to. If no log files paths are given then no file logging will be 
done. If active, the file logging will be done with a RotatingFileHandler, i.e. 
the files will be rotated when they reach a certain size. The threshold size is 
determined by ``max_bytes`` and the maximum number of files which are kept after 
rotation is controlled by ``backup_count``, both strings. For more information 
on this please refer to the `python logging docs <https://docs.python.org/3/library/logging.handlers.html#logging.handlers.RotatingFileHandler>`_. 

As stated, these all set the default log options for all publishers and 
consumers within the NLDS - these can be overridden on a consumer-specific basis 
by inserting a ``logging`` sub-dictionary into a consumer-specific optional 
section. Each sub-dictionary has identical configuration options to those listed 
above.

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
inserted into any consumer-specific config section to override this. 

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

    "nlds_q": {
        "logging": [standard_logging_dictionary],
        "retry_delays": List[int],
        "print_tracebacks_fl": boolean
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

    "index_q": {
        "logging": {standard_logging_dictionary},
        "retry_delays": List[int],
        "print_tracebacks_fl": boolean,
        "filelist_max_length": int,
        "message_threshold": int,
        "max_retries": int,
        "check_permissions_fl": boolean,
        "check_filesize_fl": boolean,
        "use_pwd_gid_fl": boolean
    }

where ``logging``, ``retry_delays``, and ``print_tracebacks_fl`` are, as above,
standard configurables within the NLDS consumer ecosystem. 
``filelist_maxlength`` determines the maximum length that any file-list provided 
to the indexer consumer during the `init` (i.e. `split`) step can be. Any 
transaction that is given initially with a list that is longer than this value 
will be split down into many sub-transactions with this as a maximum length. For 
example, with the default value of 1000, and a transaction with an initial list 
size of 2500, will be split into 3 sub-transactions; 2 of them having a 
list of 1000 files and the remaining 500 files being put into the third 
sub-transaction. 

``message threshold`` is very similar in that it places a limit on the total 
size of files within a given filelist. It is applied at the indexing 
(`nlds.index`) step when files have actually been statted, and so will further 
sub-divide any sub-transactions at that point if they are too large or are 
revealed to contain lots of folders with files in upon indexing. ``max_retries``
control the maximum number of times an entry in a filelist can be attempted to 
be indexed, either because it doesn't exist or the user doesn't have the 
appropriate permissions to access it at time of indexing. This feeds into retry 
delays, as each subsequent time a sub-transaction is retried it will be delayed 
by the amount specified at that index within the ``retry_delays`` list. If 
``max_retries`` exceeds ``len(retry_delays)``, then any retries which don't have 
an explicit retry delay to use will use the final element in the ``retry_delays`` 
list.

``check_permissions_fl`` and ``check_filesize_fl`` are commonly used boolean 
flags to control whether the indexer checks the permissions and filesize of 
files respectively during the indexing step. 

``use_pwd_gid_fl`` is a final boolean flag which controls how permissions 
checking goes about getting the gid to check group permissions against. If True, 
it will _just_ use the gid found in the ``pwd`` table on whichever machine the 
indexer is running on. If false, then this gid is used `as well as` all of those 
found using the ``os.groups`` command - which will read all groups found on the 
machine the indexer is running on. 
 

Cataloguer
^^^^^^^^^^

The server config entry for the catalog consumer is as follows::

    "catalog_q": {
        "logging": {standard_logging_dictionary},
        "retry_delays": List[int],
        "print_tracebacks_fl": boolean,
        "max_retries": int,
        "db_engine": str,
        "db_options": {
            "db_name" : str,
            "db_user" : str,
            "db_passwd" : str,
            "echo": boolean
        },
        default_tenancy: str,
        default_tape_url: str
    }

where ``logging``, ``retry_delays``, and ``print_tracebacks_fl`` are, as above,
standard configurables within the NLDS consumer ecosystem. ``max_retries`` is 
similarly available in the cataloguer, with the same meaning as defined above. 

Here we also have two keys which control database behaviour via SQLAlchemy: 
``db_engine`` and ``db_options``. ``db_engine`` is a string which specifies 
which SQL flavour you would like SQLAlchemy. Currently this has been tried with 
SQLite and PostgreSQL but, given how SQLAlchemy works, we expect few roadblocks 
interacting with other database types. ``db_options`` is a further 
sub-dictionary specifying the database name (which must be appropriate for 
your chosen flavour of database), along with the database username and password 
(if in use), respectively controlled by the keys ``db_name``, ``db_user``, and 
``db_password``. Finally in this sub-dictionary ``echo``, an optional 
boolean flag which controls the auto-logging of the SQLAlchemy engine. 

Finally ``default_tenancy`` and ``default_tape_url`` are the default values to 
place into the Catalog for a new Location's ``tenancy`` and ``tape_url`` values 
if not explicitly defined before reaching the catalog. This will happen if the 
user, for example, does not define a tenancy in their client-config. 


Transfer-put and Transfer-get
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The server entry for the transfer-put consumer is as follows::

    "transfer_put_q": {
        "logging": {standard_logging_dictionary},
        "max_retries": int,
        "retry_delays": List[int],
        "print_tracebacks_fl": boolean,
        "filelist_max_length": int,
        "check_permissions_fl": boolean,
        "use_pwd_gid_fl": boolean,
        "tenancy": str,
        "require_secure_fl": false
    }

where we have ``logging``, ``retry_delays`` and ``print_tracebacks_fl`` as their
standard definitions defined above, and ``max_retries``, ``filelist_max_length``
, and ``check_permissions_fl`` defined the same as for the Indexer consumer. 

New definitions for the transfer processor are the ``tenancy`` and 
``require_secure_fl``, which control ``minio`` behaviour. ``tenancy`` is a 
string which denotes the address of the object store tenancy to upload/download 
files to/from (e.g. `<cedadev-o.s3.jc.rl.ac.uk>`_), and ``require_secure_fl`` 
which specifies whether or not you require signed ssl certificates at the 
tenancy location. 

The transfer-get consumer is identical except for the addition of config 
controlling the change-ownership functionality on downloaded files – see 
:ref:`chowning` for details on why this is necessary. The additional config is 
as follows::

    "transfer_get_q": {
        ...
        "chown_fl": boolean,
        "chown_cmd": str
    }

where ``chown_fl`` is a boolean flag to specify whether to attempt to ``chown`` 
files back to the requesting user, and ``chown_cmd`` is the name of the 
executable to use to ``chown`` said file. 


Monitor
^^^^^^^

The server config entry for the monitor consumer is as follows::

    "monitor_q": {
        "logging": {standard_logging_dictionary},
        "retry_delays": List[int],
        "print_tracebacks_fl": boolean,
        "db_engine": str,
        "db_options": {
            "db_name" : str,
            "db_user" : str,
            "db_passwd" : str,
            "echo": boolean
        }
    }

where ``logging``, ``retry_delays``, and ``print_tracebacks_fl`` have the 
standard, previously stated definitions, and ``db_engine`` and ``db_options`` 
are as defined for the Catalog consumer - due to the use of an SQL database on 
the Monitor. Note the minimal retry control, as the monitor only retries 
messages which failed due to an unexpected exception. 

Logger
^^^^^^

The server config entry for the Logger consumer is as follows::

    "logging_q": {
        "logging": {standard_logging_dictionary},
        "print_tracebacks_fl": boolean,
    }

where the options have been previously defined. Note that there is no special 
configurable behaviour on the Logger consumer as it is simply a relay for 
redirecting logging messages into log files. It should also be noted that the 
``log_files`` option should be set in the logging sub-dictionary for this to 
work properly, which may be a mandatory setting in future versions. 


Archive-Put and Archive-Get
^^^^^^^^^^^^^^^^^^^^^^^^^^^

And, finally, the server config entry for the archive-put consumer is as 
follows::

    "archive_put_q": {
        "logging": {standard_logging_dictionary}
        "max_retries": int,
        "retry_delays": List[int],
        "print_tracebacks_fl": boolean,
        "tenancy": str,
        "check_permissions_fl": boolean,
        "require_secure_fl": boolean,
        "tape_url": str,
        "tape_pool": str,
        "query_checksum_fl": boolean,
        "chunk_size": int
    }

which is a combination of standard configuration, object-store configuration and 
as-yet-unseen tape configuration. Firstly, we have the standard options 
``logging``, ``max_retries``, ``retry_delays``, and ``print_tracebacks_fl``, 
which we have defined above. Then we have the object-store configuration options 
which we saw previously in the :ref:`transfer_put_get` consumer config, and have 
the same definitions. 

The latter four options control tape configuration, ``taoe_url`` and 
``tape_pool`` defining the ``xrootd`` url and tape pool at which to attempt to 
put files onto - note that these two values are combined together into a single 
``tape_path`` in the archiver. ``query_checksum`` is the next option, is a 
boolean flag to control whether the ADLER32 checksum calculated during streaming 
is used to check file integrity at the end of a write. Finally ``chunk_size`` is 
the size, in bytes, to chunk the stream into when writing into or reading from 
the CTA cache. This defaults to 5 MiB as this is the lower limit for 
``part_size`` when uploading back to object-store during an archive-get, but has 
not been properly benchmarked or optimised yet. 

Note that the above has been listed for the archive-put consumer but are 
shared by the archive-get consumer. The archive-get does have one additional 
config option::

    "archive_get_q": {
        ...
        "prepare_requeue": int
    }

where ``prepare_requeue`` is the prepare-requeue delay, i.e. the delay, in 
milliseconds, before an archive recall message is requeued following a negative 
read-preparedness query has been made. This defaults to 30 seconds.
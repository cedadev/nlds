
Examples
========

Local NLDS
----------

What follows is an example server config file to use for a local, development 
version of an NLDS system where all consumers are running concurrently on one 
machine - likely a laptop or single vm. This file would be saved at 
``/etc/server_config``::

    {
        "authentication" : {
            "authenticator_backend" : "jasmin_authenticator",
            "jasmin_authenticator" : {
                "user_profile_url" : "[REDACTED]",
                "user_services_url" : "[REDACTED]",
                "oauth_token_introspect_url" : "[REDACTED]"
            }
        },
        "index_q":{
            "logging":{
                "enable": true
            },
            "filelist_threshold": 10000,
            "check_permissions_fl": true,
            "max_filesize": 5000000
        },
        "nlds_q":{
            "logging":{
                "enable": true
            }
        },
        "transfer_put_q":{
            "logging":{
                "enable": true
            },
            "tenancy": "example-tenancy.s3.uk",
            "require_secure_fl": false
        },
        "transfer_get_q":{
            "logging":{
                "enable": true
            },
            "tenancy": "example-tenancy.s3.uk",
            "require_secure_fl": false
        },
        "monitor_q":{
            "db_engine": "sqlite",
            "db_options": {
                "db_name" : "//home/nlds/nlds_monitor.db",
                "db_user" : "",
                "db_passwd" : "",
                "echo": false
            },
            "logging":{
                "enable": true
            }
        },
        "logging":{
            "log_level": "debug"
        },
        "logging_q":{
            "logging":{
                "log_level": "debug",
                "add_stdout_fl": false,
                "stdout_log_level": "warning",
                "log_files": [
                    "logs/nlds_q.txt",
                    "logs/index_q.txt",
                    "logs/catalog_q.txt", 
                    "logs/monitor_q.txt",
                    "logs/transfer_put_q.txt",
                    "logs/transfer_get_q.txt",
                    "logs/logging_q.txt",
                    "logs/api_server.txt",
                    "logs/archive_put_q.txt",
                    "logs/archive_get_q.txt"
                ],
                "max_bytes": 33554432,
                "backup_count": 0
            }
        },
        "catalog_q":{
            "db_engine": "sqlite",
            "db_options": {
                "db_name" : "//home/nlds/nlds_catalog.db",
                "db_user" : "",
                "db_passwd" : "",
                "echo": false
            },
            "logging":{
                "enable": true
            },
            "default_tape_url": "root://example-tape.endpoint.uk//eos/ctaeos/cta/nlds",
            "default_tenancy": "example-tenancy.s3.uk",
        },
        "archive_get_q": {
            "tape_url": "root://example-tape.endpoint.uk//eos/ctaeos/cta/nlds",
            "tape_pool": "",
            "chunk_size": 262144,
            "tenancy": "example-tenancy.s3.uk",
            "print_tracebacks_fl": false,
            "check_permissions_fl": false,
            "require_secure_fl": false,
            "logging": {
                "enable": true
            }
        },
        "archive_put_q": {
            "query_checksum_fl": true,
            "tape_url": "root://example-tape.endpoint.uk//eos/ctaeos/cta/nlds",
            "tape_pool": "",
            "chunk_size": 262144,
            "tenancy": "example-tenancy.s3.uk",
            "print_tracebacks_fl": false,
            "check_permissions_fl": false,
            "require_secure_fl": false,
            "logging": {
                "enable": true
            }
        },
        "rabbitMQ": {
            "user": "[REDACTED]",
            "password": "[REDACTED]",
            "heartbeat": 5,
            "server": "[REDACTED]",
            "vhost": "delayed-nlds",
            "admin_port": 15672,
            "exchange": {
                "name": "test_exchange",
                "type": "topic",
                "delayed": true
            },
            "queues": [
                {
                    "name": "nlds_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "nlds-api.route.*"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "nlds-api.*.complete"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "nlds-api.*.reroute"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "nlds-api.*.failed"
                        }
                    ]
                },
                {
                    "name": "monitor_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.monitor-put.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.monitor-get.start"
                        }
                    ]
                },
                {
                    "name": "index_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.index.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.index.init"
                        }
                    ]
                },
                {
                    "name": "catalog_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.catalog-put.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.catalog-get.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.catalog-del.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.catalog-archive-next.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.catalog-archive-del.start"
                        },
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.catalog-archive-update.start"
                        }
                    ]
                },
                {
                    "name": "transfer_put_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.transfer-put.start"
                        }
                    ]
                },
                {
                    "name": "transfer_get_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.transfer-get.start"
                        }
                    ]
                },
                {
                    "name": "logging_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.log.*"
                        }
                    ]
                },
                {
                    "name": "archive_get_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.archive-get.start"
                        }
                    ]
                },
                {
                    "name": "archive_put_q",
                    "bindings": [
                        {
                            "exchange": "test_exchange",
                            "routing_key": "*.archive-put.start"
                        }
                    ]
                }
            ]
        },
        "rpc_publisher": {
            "queue_exclusivity_fl": true
        },
        "cronjob_publisher": {
            "access_key": "[REDACTED]",
            "secret_key": "[REDACTED]",
            "tenancy": "example-tenancy.s3.uk"
        }
    }


Note that this is purely illustrative and doesn't necessarily use all features 
within the NLDS - it is provided as a reference for making a new working server 
config. Note also that certain sensitive information is redacted for security 
purposes.

Distributed NLDS
----------------

When making the config for a distributed NLDS, the above would need to be split 
into the appropriate sections for each of the distributed parts being run 
separately, namely by the consumer-specific and publisher-specific sections. 
Each consumer needs the core, required ``authentication`` and ``rabbitMQ``, 
optionally ``logging`` or ``general`` config and then whatever consumer-specific
values necessary to change from default values.  

The following is a breakdown of how it might be achieved:

API-Server
^^^^^^^^^^

This would only contain the required sections as well as, optionally, any config 
for the ``rpc_publisher``::

    {
        "authentication": {
            "authenticator_backend": "jasmin_authenticator",
            "jasmin_authenticator": {
                "user_profile_url" : "[REDACTED]",
                "user_services_url" : "[REDACTED]",
                "oauth_token_introspect_url" : "[REDACTED]"
            }
        },
        "rabbitMQ": {
            "user": "[REDACTED]",
            "password": "[REDACTED]",
            "heartbeat": 5,
            "server": "[REDACTED]",
            "vhost": "nlds_staging",
            "admin_port": 15672,
            "exchange": {
                "name": "nlds",
                "type": "topic",
                "delayed": true
            },
            "queues": []
        },
        "rpc_publisher": {
            "time_limit": 60
        }
    }

NLDS Worker
^^^^^^^^^^^

This, again, contains the required sections, as well as consumer specific config 
for the NLDS-Worker. In this case the additional info would be enabling the 
logging at ``debug`` level and defining the bindings (routing keys) for the 
consumer's queue.

.. code-block:: json

    {
        "authentication": {
            "authenticator_backend": "jasmin_authenticator",
            "jasmin_authenticator": {
                "user_profile_url" : "[REDACTED]",
                "user_services_url" : "[REDACTED]",
                "oauth_token_introspect_url" : "[REDACTED]"
            }
        },
        "rabbitMQ": {
            "user": "[REDACTED]",
            "password": "[REDACTED]",
            "heartbeat": 5,
            "server": "[REDACTED]",
            "vhost": "nlds_staging",
            "admin_port": 15672,
            "exchange": {
                "name": "nlds",
                "type": "topic",
                "delayed": true
            },
            "queues": [
                {
                    "name": "nlds_q",
                    "bindings": [
                        {
                            "exchange": "nlds",
                            "routing_key": "nlds-api.route.*"
                        },
                        {
                            "exchange": "nlds",
                            "routing_key": "nlds-api.*.complete"
                        },
                        {
                            "exchange": "nlds",
                            "routing_key": "nlds-api.*.failed"
                        }
                    ]
                }
            ]
        },
        "logging": {
            "log_level": "debug"
        },
        "nlds_q": {
            "logging": {
               "enable": true
            }
        }
    }

Every other consumer would be populated similarly. 

.. note:: 
    In the production deployment of NLDS, this is practically achieved through 
    ``helm`` and the combination of different yaml config files. Please see the 
    :doc:`../deployment` documentation for more details on the practicalities of 
    deploying the NLDS.  

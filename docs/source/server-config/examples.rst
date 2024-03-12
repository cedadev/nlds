
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
        "general": {
            "retry_delays": [
                1, 5, 10, 20, 30, 60, 120, 240, 480
            ]
        },
        "index_q":{
            "logging":{
                "enable": true
            },
            "filelist_threshold": 10000,
            "check_permissions_fl": true,
            "max_filesize": 5000000,
            "retry_delays": [
                0, 0, 0
            ]
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
            "require_secure_fl": false,
            "retry_delays": [
                0, 1, 2
            ]
        },
        "transfer_get_q":{
            "logging":{
                "enable": true
            },
            "tenancy": "example-tenancy.s3.uk",
            "require_secure_fl": false,
            "retry_delays": [
                10,
                20,
                30
            ]
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
            "retry_delays": [
                0,
                0,
                0
            ],
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
            "max_retries": 5,
            "retry_delays": [0.0, 0.0, 0.0],
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
            "max_retries": 1,
            "retry_delays": [0.0, 0.0, 0.0],
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

COMING SOON
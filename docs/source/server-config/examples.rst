
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
            "use_pwd_gid_fl": true,
            "retry_delays": [
                0,
                1,
                2
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
            "use_pwd_gid_fl": true,
            "retry_delays": [
                0,
                1,
                2
            ]
        },
        "transfer_get_q":{
            "logging":{
                "enable": true
            },
            "tenancy": "example-tenancy.s3.uk",
            "require_secure_fl": false,
            "use_pwd_gid_fl": true
        },
        "monitor_q":{
            "db_engine": "sqlite",
            "db_options": {
                "db_name" : "//Users/jack.leland/nlds/nlds_monitor.db",
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
                    "logs/api_server.txt"
                ]
            }
        },
        "catalog_q":{
            "db_engine": "sqlite",
            "db_options": {
                "db_name" : "//Users/jack.leland/nlds/nlds_catalog.db",
                "db_user" : "",
                "db_passwd" : "",
                "echo": false
            },
            "retry_delays": [
                0,
                1,
                2
            ],
            "logging":{
                "enable": true
            }
        },
        "rabbitMQ": {
            "user": "full_access",
            "password": "passwordletmein123",
            "server": "130.246.3.98",
            "vhost": "delayed-test",
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
                }
            ]
        },
        "rpc_publisher": {
            "queue_exclusivity_fl": true
        }
    }

Note that this is purely an example and doesn't necessarily use all features 
within the NLDS. For example, several individual consumers have ``retry_delays``
set but not generic ``retry_delays`` is set in the ``general`` section. Note 
also that the jasmin authenication configuration is redacted for security 
purposes.

Distributed NLDS
----------------

COMING SOON
## Replacement functions for those in local_server_config.py
# So that the tests can run

def get_cronjob_dictionary():
    cj_dict = {
        "cronjob_publisher_access_key" : "fake_key",
        "cronjob_publisher_secret_key" : "fake_secret",
    }
    return cj_dict

def get_rpc_dictionary():
    rpc_dict = {
        "rpc_publisher_time_limit" : 30,
        "queue_exclusivity_fl" : True
    }
    return rpc_dict

def get_authentication_dictionary():
    ad_dict = {
        "authenticator" : "fake_authenticator",
        "user_profile_url" : "fake_user_profile_url",
        "user_services_url" : "fake_user_services_url",
        "token_introspect_url" : "fake_token_url"
    }
    return ad_dict

def get_rabbit_dictionary():
    rb_dict = {
        "rabbit_user" : "fake_user",
        "rabbit_password" : "fake_password",
        "rabbit_heartbeat" : 300,
        "rabbit_server" : "fake_server",
        "rabbit_port" : 15672,
        "rabbit_vhost" : "fake_vhost",
        "rabbit_exchange_name" : "fake_exchange",
        "rabbit_exchange_type" : "topic",
        "rabbit_exchange_delayed" : True
    }
    return rb_dict

def get_database_dictionary():
    db_dict = {
        "catalog_q_db_engine" : "sqlite",
        "catalog_q_db_name" : "",
        "catalog_q_db_user" : "",
        "catalog_q_db_passwd" : "",

        "monitor_q_db_engine" : "sqlite",
        "monitor_q_db_name" : "",
        "monitor_q_db_user" : "",
        "monitor_q_db_passwd" : "",
        "db_echo" : False
    }
    return db_dict

def get_config_dictionary():
    """Get the variables for the Jinja2 template that are not secret.
       These are just specified here in the file."""
    
    config_dict = {
        "template_file_location" : "./server_config",
        "log_file_location" : "fake_log",

        "tenancy" : "fake_tenancy",
        "tape_url" : "fake_url",
        "tape_pool" : None,
        "chunk_size" : 262144,
        "filelist_max_length" : 1000,
        "filelist_max_size" : 16 * 1000 * 1000,
        "print_tracebacks" : False,
        "check_filesize" : True,
        "require_secure" : False,

        "log_level" : "debug",
        "logging_enable" : True,
        "logging_add_stdout" : False,
        "logging_stdout_log_level" : None,
        "logging_max_bytes" : 16 * 1000 * 1000,
        "logging_backup_count" : 0,

        "rabbit_exchange_name" : "fake_exchange"
    }
    return config_dict
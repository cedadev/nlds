"""Once filled out, keep this file private, add it to .gitignore!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!!! DO NOT CHECK THIS FILE INTO GITHUB WITH ANY SENSITIVE INFORMATION !!!!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

define these functions:

get_cronjob_dictionary
get_rpc_dictionary
get_authentication_dictionary
get_rabbit_dictionary
get_database_dictionary
get_config_dictionary

"""

def get_cronjob_dictionary():
    cj_dict = {
        "cronjob_publisher_access_key" : "",
        "cronjob_publisher_secret_key" : "",
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
        "authenticator" : "jasmin_authenticator",
        "user_profile_url" : "",
        "user_services_url" : "",
        "token_introspect_url" : ""
    }
    return ad_dict

def get_rabbit_dictionary():
    rb_dict = {
        "rabbit_user" : "",
        "rabbit_password" : "",
        "rabbit_heartbeat" : 300,
        "rabbit_server" : "",
        "rabbit_port" : 1,
        "rabbit_vhost" : "",
        "rabbit_exchange_name" : "",
        "rabbit_exchange_type" : "",
        "rabbit_exchange_delayed" : True
    }
    return rb_dict

def get_database_dictionary():
    db_dict = {
        "catalog_q_db_engine" : "",
        "catalog_q_db_name" : "",
        "catalog_q_db_user" : "",
        "catalog_q_db_passwd" : "",

        "monitor_q_db_engine" : "",
        "monitor_q_db_name" : "",
        "monitor_q_db_user" : "",
        "monitor_q_db_passwd" : "",
        "db_echo" : False
    }
    return db_dict

def get_config_dictionary():
    config_dict = {
        "template_file_location" : "../server_config",
        "log_file_location" : "",

        "tenancy" : "",
        "tape_url" : "",
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

        "rabbit_exchange_name" : ""
    }
    return config_dict
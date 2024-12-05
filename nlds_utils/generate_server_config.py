#! /usr/bin/env python
# encoding: utf-8
"""Generate a server config from the Jinja2 (.j2) config files in the 
server_config/ directory"""

__author__ = "Neil Massey"
__date__ = "27 Nov 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import click
from jinja2.nativetypes import NativeEnvironment
from jinja2 import FileSystemLoader
import os.path
import json

from local_server_config import (
    get_config_dictionary, get_database_dictionary, get_rabbit_dictionary,
    get_authentication_dictionary, get_rpc_dictionary, get_cronjob_dictionary
)

all_processors = [
    "archive_get_q",
    "archive_put_q",
    "catalog_q",
    "index_q",
    "logging_q",
    "monitor_q",
    "nlds_q",
    "transfer_del_q",
    "transfer_get_q",
    "transfer_put_q",
]

@click.command()
@click.option(
    "-p",
    "--process",
    default="all",
    type=str,
    help="The nlds process to generate the config file for.",
    required = False
)
@click.option(
    "-o",
    "--output",
    type=str,
    help="Output path to write the config file to.",
    required = False,
    default=None
)
def generate_server_config(process: str, output: str) -> None:
    # get which processes to render configs for
    if process == "all":
        processors = all_processors
    else:
        if not process in all_processors:
            raise RuntimeError(f"Did not recognise process: {process}")
        else:
            processors = [process]

    # get the individual dictionaries then merge them
    config = get_config_dictionary()
    db_dict = get_database_dictionary()
    rb_dict = get_rabbit_dictionary()
    auth_dict = get_authentication_dictionary()
    cj_dict = get_cronjob_dictionary()
    rpc_dict = get_rpc_dictionary()

    config.update(db_dict)
    config.update(rb_dict)
    config.update(auth_dict)
    config.update(cj_dict)
    config.update(rpc_dict)

    # use the config dictionary to get the template file and log file locations
    template_file_location = config["template_file_location"]

    # load the server_config.j2 top level config file as JSON dictionary
    filepath = os.path.join(template_file_location)
    loader = FileSystemLoader(searchpath=filepath)
    nenv = NativeEnvironment(loader=loader)
    # add the log files to the dictionary
    config["log_files"] = [
        os.path.join(config["log_file_location"], process) for process in processors
    ]

    # render the server_config
    server_template = nenv.get_template(name="server_config.j2")
    server_config = server_template.render(config)
    # now render each process' template and add to the server_config
    for process in processors:
        process_template = nenv.get_template(name=f"processors/{process}.j2")
        process_config = process_template.render(config)
        process_queue = process_config["rabbitMQ"]["queues"]
        server_config["rabbitMQ"]["queues"].extend(process_queue)
        server_config[process] = process_config[process]

    # open the output file and write out the config as json
    if output is not None:
        with open(output, "w") as out:
            json.dump(server_config, out, indent=4)
        print(f"{output} file written")
    else:
        print(json.dumps(server_config, indent=4))

if __name__ == "__main__":
    generate_server_config()

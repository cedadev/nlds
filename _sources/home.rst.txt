CEDA Near-Line Data Store
=========================

[![Testing](https://github.com/cedadev/nlds/actions/workflows/ci.yml/badge.svg)](https://github.com/cedadev/nlds/actions/workflows/ci.yml)
[![Docs](https://github.com/cedadev/nlds/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/cedadev/nlds/actions/workflows/pages/pages-build-deployment)
[![PEP8](https://img.shields.io/badge/code%20style-pep8-orange.svg)](https://www.python.org/dev/peps/pep-0008/)
[![Coverage](https://cedadev.github.io/nlds/coverage.svg)](https://cedadev.github.io/nlds/coverage/htmlcov/)

This is the HTTP API server code for the CEDA Near-Line Data Store (NLDS).
It requires the use of the NLDS client, either the command line or library:
[NLDS client on GitHub](https://github.com/cedadev/nlds-client).

NLDS server is built upon [FastAPI](https://fastapi.tiangolo.com).

NLDS requires Python 3.  It has been tested with Python 3.9, 3.10 and Python 3.11.

Installation
------------

1.  Create a Python virtual environment:
    `python3 -m venv ~/nlds-venv`

2.  Activate the nlds-venv:
    `source ~/nlds-venv/bin/activate`

3.  Install the nlds package with editing capability:
    `pip install -e ~/Coding/nlds`

Running - Dec 2021
------------------

1. NLDS currently uses `uvicorn` to run.  The command line to invoke it is:
```uvicorn nlds.main:nlds --reload```

    This will create the NLDS REST-API server at the IP-address: `http://127.0.0.1:8000/`

2. To run the processors, you have two options:
    1. In unique terminals start each processor individually, after 
    activating the virtual env, for example:
       ```source ~/nlds-venv/bin/activate; python nlds_processors/index.py```
       This will send the output to the terminal.

    2. Use the script `test_run_processor.sh`.  This will run all five processors
       in the background, sending the output to five logs in the `~/nlds_log/`
       directory.

Viewing the API docs
--------------------

FastAPI displays automatically generated documentation for the REST-API.  To browse this go to: http://127.0.0.1:8000/docs#/

Server Config
-------------

To interface with the JASMIN accounts portal, for the OAuth2 authentication, a `.server_config` file has to be created.  This contains infrastructure information and so is not included in the GitHub repository.

A Jinja-2 template for the `.server_config` file can be found in the `templates/` directory.

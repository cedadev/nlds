CEDA Near-Line Data Store
=========================

[![Testing](https://github.com/cedadev/nlds/actions/workflows/ci.yml/badge.svg)](https://github.com/cedadev/nlds/actions/workflows/ci.yml)
[![Docs](https://github.com/cedadev/nlds/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/cedadev/nlds/actions/workflows/pages/pages-build-deployment)
[![PEP8](https://img.shields.io/badge/code%20style-pep8-orange.svg)](https://www.python.org/dev/peps/pep-0008/)
[![Coverage](https://cedadev.github.io/nlds/coverage.svg)](https://cedadev.github.io/nlds/coverage/htmlcov/)

This is the server code for the CEDA Near-Line Data Store (NLDS), consisting of 
an HTTP API and a cluster of rabbit consumer microservices. The 
[NLDS client](https://github.com/cedadev/nlds-client) is required to communicate 
with the API, either via the command line interface or python client library.

The NLDS is a unified storage solution, allowing easy use of disk, s3 object 
storage, and tape from a single interface. It utilises object storage as a cache
for the tape backend allowing for low-latency backup 

The NLDS server is built upon [FastAPI](https://fastapi.tiangolo.com) for the 
API, [RabbitMQ](https://www.rabbitmq.com/) for the message broker, 
[minio](https://min.io/) for the s3 client, 
[SQLAlchemy](https://www.sqlalchemy.org/) for the database client and 
[xrootd](https://xrootd.slac.stanford.edu/) for the tape interactions.

Documentation can be found [here](https://cedadev.github.io/nlds/index.html).

Installation
------------

If installing locally we strongly recommend the use of a virtual environment to 
manage the dependencies.

1.  Create a Python virtual environment:
   
    ```
    python3 -m venv nlds-venv
    ```

2.  Activate the nlds-venv:

    ```
    source ~/nlds-venv/bin/activate
    ```

3.  You could either install the nlds package with editing capability from a 
    locally cloned copy of this repo (note the inclusion of the editable flag 
    `-e`), e.g.

    ```
    pip install -e ~/Coding/nlds
    ```

    or install this repo directly from github:

    ```
    pip install git+https://github.com/cedadev/nlds.git
    ```

4.  (Optional) There are several more requirements/dependencies defined:
    *   `requirements-dev.txt` - contains development-specific (i.e. not 
    production appropriate) dependencies. Currently this consists of a psycopg2 
    binary python package for interacting with PostgeSQL from a local NLDS 
    instance. 
    *   `requirements-deployment.txt` - contains deployment-specific 
    dependencies, excluding `XRootD`. Currently this consists of the psycopg2 
    package but built from source instead of a precompiled binary. 
    *   `requirements-tape.txt` - contains tape-specific dependencies, notably 
    `XRootD`. 
    *   `tests/requirements.txt` - contains the dependencies for the test suite. 
    *   `docs/requirements.txt` - contains the dependencies required for 
    building the documentation with sphinx.

Server Config
-------------

To interface with the JASMIN accounts portal, for the OAuth2 authentication, a 
`.server_config` file has to be created. This contains infrastructure 
information and so is not included in the GitHub repository. See the 
[relevant documentation](https://cedadev.github.io/nlds/server-config/server-config.html) 
and [examples](https://cedadev.github.io/nlds/server-config/examples.html) for 
more information.

A Jinja-2 template for the `.server_config` file can also be found in the 
`templates/` directory.

Running the Server
------------------

1.  The NLDS API requires something to serve the API, usually uvicorn in a local 
    development environment:

    ```
    uvicorn nlds.main:nlds --reload
    ```

    This will create a local NLDS API server at `http://127.0.0.1:8000/`. 
    FastAPI displays automatically generated documentation for the REST-API, to 
    browse this go to http://127.0.0.1:8000/docs/

2.  To run the microservices, you have three options:
    1.  In individual terminals, after activating the virtual env, (e.g. 
        `source ~/nlds-venv/bin/activate`), start each of the microservice 
        consumers:
        ```
        nlds_q
        index_q
        catalog_q  
        transfer_put_q   
        transfer_get_q
        logging_q
        monitor_q
        archive_put_q
        archive_get_q
        ```
        This will send the output of each consumer to its own terminal (as well 
        as whatever is configured in the logger).

    2.  Alternatively, you can use the scripts in the `test_run/` directory: 
        `start_test_run.py` to start and `stop_test_run.py` to stop. 
        This will start a [screen](https://www.gnu.org/software/screen/manual/screen.html) 
        session with all 8 processors (+ api server) in, sending each output to 
        a log in the `./nlds_log/` directory.
        This method is good for getting a whole NLDS infrastructure up and running
        quickly, but is not so great for debugging.

    3.  Also in the `test_run/` directory is a script called `nlds-up` which will
    activate the virtual environment and run one of the microservices listed above.
    It requires the enviroment variable `NLDS_VENV` to be set to point to the 
    virtual environemnt created above (e.g. `~/nlds-venv/` - however, user paths 
    have to be expanded so, for the Mac, this would be 
    `/Users/<your_username>/nlds-venv`).
    It is recommended to set the environment variable in your shell profile.  For **bash** this is `~/.bash_profile`, and the environment can be set using the 
    line: 
    ```
    export NLDS_VENV="/Users/<your_username>/python-venvs/nlds-venv"
    ```
    Running `nlds-up` then takes the name of one of the microservices above: e.g. 
    `nlds-up catalog_q`


Tests
-----

The NLDS uses pytest for its unit test suite. Once `test/requirements.txt` have 
been installed, you can run the tests with 
```
pytest
```
in the root directory. Pytest is also used for integration testing in the 
separate [nlds-test repo](https://github.com/cedadev/nlds-test). 

The `pytest` test-coverage report can (hopefully) be found [here](https://cedadev.github.io/nlds/coverage/htmlcov/).


License
-------

The NLDS is available on a BSD 2-Clause License, see the [license](./LICENSE.txt) 
for more info.



Acknowledgements
================

NLDS was developed at the Centre for Environmental Data Analysis and supported 
through the ESiWACE2 project. The project ESiWACE2 has received funding from the 
European Union's Horizon 2020 research and innovation programme under grant 
agreement No 823988.

CEDA Near-Line Data Store
=========================

This is the documentation for the server code of the CEDA Near-Line Data Store 
(NLDS), consisting of an HTTP API and a cluster of rabbit consumer microservices. 
The `NLDS client <https://github.com/cedadev/nlds-client>`_ is required to 
communicate with the API, either via the command line interface or python client 
library.

The NLDS is a unified storage solution, allowing easy use of disk, s3 object 
storage, and tape from a single interface. It utilises object storage as a cache
for the tape backend allowing for low-latency backup. 

The NLDS server is built upon `FastAPI <https://fastapi.tiangolo.com>`_ for the 
API, `RabbitMQ <https://www.rabbitmq.com/>`_ for the message broker, 
`minio <https://min.io/>`_ for the s3 client, 
`SQLAlchemy <https://www.sqlalchemy.org/>`_ for the database client and 
`xrootd <https://xrootd.slac.stanford.edu/>`_ for the tape interactions.

Installation
------------

If installing locally we strongly recommend the use of a virtual environment to 
manage the dependencies.

1.  Create a Python virtual environment::

        python3 -m venv nlds-venv

3.  Activate the nlds-venv::

        source nlds-venv/bin/activate

4.  You could either install the nlds package with editing capability from a 
    locally cloned copy of this repo (note the inclusion of the editable flag 
    `-e`), e.g.::

        pip install -e ~/Coding/nlds


    or install this repo directly from github::

        pip install git+https://github.com/cedadev/nlds.git
    

5.  (Optional) There are several more requirements/dependencies defined:
    
    *   ``requirements-dev.txt`` - contains development-specific (i.e. not 
        production appropriate) dependencies. Currently this consists of a psycopg2 
        binary python package for interacting with PostgeSQL from a local NLDS 
        instance. 
    
    *   ``requirements-deployment.txt`` - contains deployment-specific 
        dependencies, excluding ``XRootD``. Currently this consists of the psycopg2 
        package but built from source instead of a precompiled binary. 
    
    *   ``requirements-tape.txt`` - contains tape-specific dependencies, notably 
        ``XRootD``. 
    
    *   ``tests/requirements.txt`` - contains the dependencies for the test suite. 
    
    *   ``docs/requirements.txt`` - contains the dependencies required for 
        building the documentation with sphinx.


Server Config
-------------

To interface with the JASMIN accounts portal, for the OAuth2 authentication, a 
``.server_config`` file has to be created. This contains infrastructure 
information and so is not included in the GitHub repository. See the 
`relevant documentation <https://cedadev.github.io/nlds/server-config/server-config.html>`_ 
and `examples <https://cedadev.github.io/nlds/server-config/examples.html>`_ for 
more information.

A Jinja-2 template for the ``.server_config`` file can also be found in the 
``templates/`` directory.

Running the Server
------------------

1.  The NLDS API requires something to serve the API, usually uvicorn in a local 
    development environment:

    ```
    uvicorn nlds.main:nlds --reload
    ```

    This will create a local NLDS API server at `<http://127.0.0.1:8000/>`_. 
    FastAPI displays automatically generated documentation for the REST-API, to 
    browse this go to `<http://127.0.0.1:8000/docs/>`_

2.  To run the microservices, you have two options:

    *   In individual terminals, after activating the virtual env, (e.g. 
        ``source ~/nlds-venv/bin/activate``), start each of the microservice 
        consumers::

            nlds_q
            index_q
            catalog_q  
            transfer_put_q   
            transfer_get_q
            logging_q
            archive_put_q
            archive_get_q

        This will send the output of each consumer to its own terminal (as well 
        as whatever is configured in the logger).

    *   Alternatively, you can use the scripts in the ``test_run/`` directory, 
        notably ``start_test_run.py`` to start and ``stop_test_run.py`` to stop. 
        This will start a `screen <https://www.gnu.org/software/screen/manual/screen.html>`_ 
        session with all 8 processors (+ api server) in, sending each output to 
        a log in the ``./nlds_log/`` directory.
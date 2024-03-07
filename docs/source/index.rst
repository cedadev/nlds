.. Near-line Data Store documentation master file, created by
   sphinx-quickstart on Mon Jan 31 14:54:07 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Near-line Data Store - Server Documentation
===========================================

This is the documentation for the Near-line Data Store (NLDS) server, a tool 
developed at JASMIN to provide a single interface for disk, object storage and 
tape. We also have separate documentation for the `NLDS client <https://cedadev.github.io/nlds-client/>`_.

These docs are split into a user-guide, if you plan on simply running the NLDS 
server; a development guide, for some of the specifics required to know about if 
you plan on contributing to the NLDS; and an API Reference. 

.. toctree::
   :maxdepth: 1
   :caption: User Guide

   Getting started <home>
   Using the system status page <system-status>
   The server config file <server-config/server-config>
   Server config examples <server-config/examples>
   Deployment <deployment>


.. toctree::
   :maxdepth: 1
   :caption: Development

   Specification document <specification>
   Setting up a CTA tape emulator <development/cta-emulator>
   Database Migrations with Alembic <development/alembic-migrations>
   Test coverage report <coverage/coverage-report>


.. toctree::
   :maxdepth: 1
   :caption: API Reference

   NLDS Processors <api-reference/nlds-processors>
   NLDS Server <api-reference/nlds-server>


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Acknowledgements
================

NLDS was developed at the Centre for Environmental Data Analysis and supported 
through the ESiWACE2 project. The project ESiWACE2 has received funding from the 
European Union's Horizon 2020 research and innovation programme under grant 
agreement No 823988.

.. image:: _images/esiwace2.png
   :width: 400
   :alt: ESiWACE2 Project Logo

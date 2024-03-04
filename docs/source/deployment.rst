
Deployment Description
======================

The NLDS is deployed as a collection of containers on the JASMIN rancher cluster 
'wigbiorg', a public-facing Kubernetes cluster hosted by JASMIN.

Due to the microservice architecture it was very easy to decide at what level to 
apply the containerisation - each microservice sits in its own container. There 
are therefore nine different containers that make up the deployment of the NLDS, 
eight for the consumers and one additional container for the FastAPI server:

1. FastAPI Server
2. Worker (router)
3. Indexer
4. Catalog
5. Transfer-Put
6. Transfer-Get
7. Logging
8. Archive-Put
9. Archive-Get

The FastAPI server is defined and deployed in the [``nlds-server-deploy``] 
repository in gitlab and the latter 8 are similarly defined and deployed in 
the [``nlds-consumers-deploy``] repository. All have subtly different 
configurations and dependencies, which can be gleamed in detail by looking at 
the configuration yaml files and helm chart in the repos, but are also, 
mercifully, described below.

Images
------

The above containers do not all run on the same image, but are sub-divided into 
three specific roles:

1. Generic Server: ``nlds/app``
2. Generic Consumer: ``nlds-consumers/consumer``
3. Tape Consumer: ``nlds-consumers/archiver``

The string after each of these corresponds to the image's location on CEDA's 
Harbor registry (and therefore what tag/registry address to use to ``docker 
pull`` each of them). As may be obvious, the FastAPI server runs on the 
``Generic Server`` image and contains an installation of asgi, building upon the 
``asgi`` [base-image], to actually run the server. The rest run on the ``Generic 
Consumer`` image, which has an installation of the NLDS repo, along 
with its dependencies, to allow it to run a given consumer. The only dependency 
which isn't included is xrootd as it is a very large and long installation 
process and unnecessary to the running of the non-tape consumers. Therefore the 
``Tape Consumer`` image was created, which appropriately builds upon the 
``Geneic Consumer`` image with an additional installation of ``xrootd`` with 
which to run tape commands. The two tape consumers, ``Archive-Put`` and 
``Archive-Get``, run on containers using this image.

The two consumer containers run as the user NLDS, which is an official JASMIN 
user at uid=7054096 and is baked into the container (i.e. unconfigurable).
Relatedly, every container runs with config associating the NLDS user with 
supplemental groups, the list of which constitutes every group-workspace on 
JASMIN. The list was generated with the command::
    
    ldapsearch -LLL -x -H ldap://homer.esc.rl.ac.uk -b "ou=ceda,ou=Groups,o=hpc,dc=rl,dc=ac,dc=uk"

This will need to be periodically rerun and the output reformatted to update the 
list of ``supplementalGroups`` in [this config file].

Each of the containers will also have specific config and specific deployment 
setup to help the container perform its particular task its particular task.  

Common Deployment Configurations
--------------------------------

There are several common deployment configurations (CDC) required to perform 
tasks, which some, or all, of the containers make use of to function.

The most commonly used is the ``nslcd`` pod which provides the containers with 
up-to-date uid and gid information from the LDAP servers. This directly uses the 
[``nslcd``] image developed for the notebook server, and runs as a side-car in 
every deployed pod to periodically poll the LDAP servers to provide names and 
permissions information to the main container in the pod (the consumer) so that 
file permissions can be handled properly. In other words, it ensures the 
``passwd`` file on the consumer container is up to date, and therefore that the 
aforementioned supplementalGroups are properly respected. 

Another CDC used across all pods is the rabbit configuration, details of which 
can be found in :doc:`server-config/server-config`. 

An additional CDC used by the microservices which require reading from or writing 
to the JASMIN filesystem are the filesystem mounts, which will mount the group 
workspaces (in either read or write mode) onto the appropriate path (``/gws`` or 
``group_workspaces``). This is used by the following containers:
* Transfer-Put
* Transfer-Get
* Indexer

A further CDC is the postgres configuration, which is obviously required by the 
database-interacting consumers (Catalog and Monitor) and again described in 
:doc:`server-config/server-config`. However an additional part of this process 
is running any database migrations to the database schema is up to date. This 
will be discussed in more detail in section :ref:`db_migration`.

There are some slightly more complex deployment configurations involved in the 
rest of the setup, which are described below. 

.. _tape_keys:

Tape keys
---------


.. _db_migration:

Migrations
----------


.. _logging:

Logging with Fluentbit
----------------------


.. _scaling:

Scaling
-------


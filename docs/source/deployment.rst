
Deployment
==========

The NLDS is deployed as a collection of containers on the JASMIN rancher cluster 
'wigbiorg', a public-facing Kubernetes cluster hosted by JASMIN.

Due to the microservice architecture the level to apply the containerisation was
immediately clear - each microservice sits in its own container. There are 
therefore nine different containers that make up the deployment of the NLDS, 
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

The FastAPI server is defined and deployed in the `nlds-server-deploy <https://gitlab.ceda.ac.uk/cedadev/nlds-server-deploy>`_
repository in gitlab and the latter 8 are similarly defined and deployed in 
the `nlds-consumers-deploy <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy>`_ 
repository. All have subtly different configurations and dependencies, which can 
be gleamed in detail by looking at the configuration yaml files and helm chart 
in the repos, but are also, mercifully, described below.

.. note::
    All of the following describes the deployment set up for the `production` 
    environment. The setup for the staging/beta testing environment is very 
    similar but not `quite` the same, so the differences will be summarised in 
    the :ref:`staging` section.


Images
------

The above containers do not all run on the same image, but are sub-divided into 
three specific roles:

1. `Generic Server <https://gitlab.ceda.ac.uk/cedadev/nlds-server-deploy/-/tree/master/images/Dockerfile>`_: ``nlds/app``
2. `Generic Consumer <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/tree/master/images/consumer/Dockerfile>`_: ``nlds-consumers/consumer``
3. `Tape Consumer <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/tree/master/images/archiver/Dockerfile>`_: ``nlds-consumers/archiver``

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
``/group_workspaces``). This is used by the following containers:

* Transfer-Put
* Transfer-Get
* Indexer

.. note::
    It is the intention to eventually include several more directories into the 
    mounting (``/xfc``, ``/home``) but this is not currently possible with the 
    version of Kubernetes installed on wigbiorg

A further CDC is the postgres configuration, which is obviously required by the 
database-interacting consumers (Catalog and Monitor) and, again, fully described 
in :doc:`server-config/server-config`. However an additional part of this 
process is running any database migrations to the database schema is up to date. 
This will be discussed in more detail in section :ref:`db_migration`.

There are some slightly more complex deployment configurations involved in the 
rest of the setup, which are described below. 

.. _tape_keys:

Tape Keys
---------

The CERN Tape Archive (CTA) instance at STFC requires the use of authentication 
to access the different tape pools and tape instances. This is done through 
Kerberos on the backend and requires the use of a forwardable keytab file with 
appropriate permissions. From the perspective of the NLDS this is actually quite 
simple, Scientific Computing (SCD) provide a string to put into a keytab (text) 
file which describes the CTA user and authentication and must have unix octal 
permissions 600 (i.e. strictly user read-writable). Finally two xrootd-specific 
environment variables must be created::

    XrdSecPROTOCOL=sss
    XrdSecSSSKT=path/to/keytab/file

The problem arises with the use of Kubernetes, wherein the keytab content string 
must be kept secret. This is handled in the CEDA gitlab deployment process 
through the use of git-crypt (see `here <https://gitlab.ceda.ac.uk/cedaci/ci-tools/-/blob/master/docs/setup-kubernetes-project.md#including-deployment-secrets-in-a-project>`_ 
for more details) to encrypt and Kubernetes secrets to decrypt at deployment 
time. Unfortunately permissions can't be set, no changed, on files made by 
Kubernetes secrets, so to get the keytab in the right place with the right 
permissions the deployment utilises an init-container to copy the secret key to 
a new file and then alter permissions on it to 600.


.. _db_migration:

Migrations
----------

As described in :doc:`development/alembic-migrations`, the NLDS uses Alembic for 
database migrations. During the deployment these are done as an initial step 
before any of the consumers are updated, so that nothing attempts to use the new 
schema before the database has been migrated, and this is implemented through 
two mechanisms in the deployment:

1. An init-container on the catalog, which has the config for both the catalog 
   and montioring dbs, which has alembic installed and calls::
        
        alembic upgrade head

2. The catalog container deployment running first (alongside the logging) before 
   all the other container deployments. 

This means that if the database migration fails for whatever reason, the whole 
deployment stops and the migration issue can be investigated through the logs. 

.. _logging:

Logging with Fluentbit
----------------------

The logging for the NLDS, as laid out in the specification, was originally 
designed to concentrate logs onto a single container for ease of perusal. 
Unfortunately, due to constraints of the Kubernetes version employed, the 
container has only limited, temporary storage capacity (the memory assigned from 
the cluster controller) and no means of attaching a more persistent volume to 
store logs in long-term. 

The, relatively new, solution that exists on the CEDA cluster is the use of 
`fluentd`, and more precisely `fluentbit <https://fluentbit.io/how-it-works/>`_, 
to aggregate logs from the NLDS logging microservice and send them to a single 
external location running `fluentd` – currently the stats-collection virtual 
machine run on JASMIN. Each log sent to the `fluentd`` service is tagged with a 
string representing the particular microservice log file it was collected from, 
e.g. the logs from the indexer microservice on the staging deployment are tagged 
as:: 

    nlds_staging_index_q_log

This is practically achieved through the use of a sidecar – a further container 
running in teh same pod as the logging container – running the fluentbit image 
as defined by the `fluentbit helm chart <https://gitlab.ceda.ac.uk/cedaci/helm-charts>`_. 
The full `fluentbit`` config, including the full list of tags, can be found `in 
the logging config yamls <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/tree/master/conf/logger>`_.
When received by the fluentd server, each tagged log is collated into a larger 
log file for help with debugging at some later date. The log files on the 
logging microservice's container are rotated according to size, and so should 
not exceed the pod's allocated memory limit.

.. note::
    The `fluentbit` service is still in its infancy and subject to change at 
    short notice as the system & helm chart get more widely adopted. For example 
    the length of time log files are kept on the stats machine has not been 
    finalised yet. 

While the above is true for long term log storage, the rancher interface for the 
Kubernetes cluster can still be used to check the output logs of each consumer 
in the standard way for quick diagnosis of problems with the NLDS.


.. _scaling:

Scaling
-------

A core part of the design philosophy of the NLDS was it's microservice 
architecture. 


.. _staging:

Staging Deployment
------------------


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
``asgi`` `base-image <https://gitlab.ceda.ac.uk/cedaci/base-images/-/tree/main/asgi>`_, 
to actually run the server. The rest run on the ``Generic Consumer`` image, 
which has an installation of the NLDS repo, along with its dependencies, to 
allow it to run a given consumer. The only dependency which isn't included is 
xrootd as it is a very large and long installation process and unnecessary to 
the running of the non-tape consumers. Therefore the ``Tape Consumer`` image was 
created, which appropriately builds upon the ``Geneic Consumer`` image with an 
additional installation of ``xrootd`` with which to run tape commands. The two 
tape consumers, ``Archive-Put`` and ``Archive-Get``, run on containers using 
this image.

The two consumer containers run as the user NLDS, which is an official JASMIN 
user at ``uid=7054096`` and is baked into the container (i.e. unconfigurable).
Relatedly, every container runs with config associating the NLDS user with 
supplemental groups, the list of which constitutes every group-workspace on 
JASMIN. The list was generated with the command::
    
    ldapsearch -LLL -x -H ldap://homer.esc.rl.ac.uk -b "ou=ceda,ou=Groups,o=hpc,dc=rl,dc=ac,dc=uk"

This will need to be periodically rerun and the output reformatted to update the 
list of ``supplementalGroups`` in `this config file <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/blob/master/conf/common.yaml?ref_type=heads#L14-515>`_.

Each of the containers will also have specific config and specific deployment 
setup to help the container perform its particular task its particular task.  

Common Deployment Configurations
--------------------------------

There are several common deployment configurations (CDC) required to perform 
tasks, which some, or all, of the containers make use of to function.

The most commonly used is the ``nslcd`` pod which provides the containers with 
up-to-date uid and gid information from the LDAP servers. This directly uses the 
`nslcd <https://gitlab.ceda.ac.uk/jasmin-notebooks/jasmin-notebooks/-/tree/master/images/nslcd>`_ 
image developed for the notebook server, and runs as a side-car in every 
deployed pod to periodically poll the LDAP servers to provide names and 
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

A further CDC is the PostgreSQL configuration, which is obviously required by 
the database-interacting consumers (Catalog and Monitor) and, again, fully 
described in :doc:`server-config/server-config`. The production system uses the 
databases ``nlds_catalog`` and ``nlds_monitor`` on the Postgres server 
``db5.ceda.ac.uk`` hosted and maintained by CEDA. However, an additional part of 
this configuration is running any database migrations to the database schema is 
up to date. This will be discussed in more detail in section 
:ref:`db_migration`.

There are some slightly more complex deployment configurations involved in the 
rest of the setup, which are described below. 

.. _api_server:

API Server
----------

The NLDS API server, as mentioned above, was written using FastAPI. In a local 
development environment this is served using ``uvicorn``, but for the production 
deployment the `base-image <https://gitlab.ceda.ac.uk/cedaci/base-images/-/tree/main/asgi>` 
base-image is used, which runs the server instead with ``guincorn``. They are 
functionally identical so this is not a problem per se, just something to be 
aware of. The NLDS API helm deployment is an extension of the standard `FastAPI helm chart <https://gitlab.ceda.ac.uk/cedaci/base-images/-/tree/main/fast-api>`_.

On production, this API server sits facing the public internet behind an NGINX 
reverse-proxy, handled by the standard `nginx helm chart <https://gitlab.ceda.ac.uk/cedaci/helm-charts/-/tree/master/nginx>`_ 
in the ``cedaci/helm-charts`` repo. It is served to the domain 
`https://nlds.jasmin.ac.uk <https://nlds.jasmin.ac.uk>`_, with the standard NLDS 
API endpoints extending from that (such as ``/docs``, ``/system/status``). The 
NLDS API also has an additional endpoint (``/probe/healthz``) for the Kubernetes 
liveness probe to periodically ping to ensure the API is alive, and that the 
appropriate party is notified if it goes down. Please note, this is not a 
deployment specific endpoint and will also exist on any local development 
instances. 


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
through the use of git-crypt (see `here <https://gitlab.ceda.ac.uk/cedaci/ci-tools/-/blob/master/docs/setup-kubernetes-project.md#including-deployment-secrets-in-a-project>`__
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
   and montioring DBs, which has alembic installed and calls::
        
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
machine run on JASMIN. Each log sent to the `fluentd` service is tagged with a 
string representing the particular microservice log file it was collected from, 
e.g. the logs from the indexer microservice on the staging deployment are tagged 
as:: 

    nlds_staging_index_q_log

This is practically achieved through the use of a sidecar – a further container 
running in the same pod as the logging container – running the ``fluentbit`` 
image as defined by the `fluentbit helm chart <https://gitlab.ceda.ac.uk/cedaci/helm-charts>`_. 
The full ``fluentbit`` config, including the full list of tags, can be found `in 
the logging config yamls <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/tree/master/conf/logger>`_.
When received by the fluentd server, each tagged log is collated into a larger 
log file for help with debugging at some later date. The log files on the 
logging microservice's container are rotated according to size, and so should 
not exceed the pod's allocated memory limit.

.. note::
    The `fluentbit` service is still in its infancy and subject to change at 
    short notice as the system & helm chart get more widely adopted. For example, 
    the length of time log files are kept on the stats machine has not been 
    finalised yet. 

While the above is true for long term log storage, the rancher interface for the 
Kubernetes cluster can still be used to check the output logs of each consumer 
in the standard way for quick diagnosis of problems with the NLDS.


.. _scaling:

Scaling
-------

A core part of the design philosophy of the NLDS was its microservice 
architecture, which allows for any of the microservices to be scaled out in an 
embarrassingly parallelisable way to meet changing demand. This is easily 
achieved in Kubernetes through simply spinning up additional containers for a 
given microservice using the ``replicaCount`` `parameter <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/blob/master/chart/values.yaml?ref_type=heads#L21>`_.
By default this value is 1 but has been increased for certain microservices 
deemed to be bottlenecks during beta testing, notably the `Transfer-Put microservice <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/blob/master/conf/transfer_put/common.yaml?ref_type=heads#L17>`_
where it is set to 8 and the Transfer-Get where is set to 2. 

.. note::
    While correct at time of writing, these values are subject to change – it 
    may be that other microservices are found which require scaling and those 
    above do not require as many replicas as currently allocated. 

    An ideal solution would be to automatically scale the deployments based on 
    the size of a ``Rabbit`` queue for a given microservice, and while this is 
    `in theory` `possible <https://ryanbaker.io/2019-10-07-scaling-rabbitmq-on-k8s/>`_,
    this was not possible with the current installation of Kubernetes without 
    additional plugins, namely `Prometheus`.

The other aspect of scaling is the resource requested by each of the pods, which 
have current `default values <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/blob/master/conf/common.yaml?ref_type=heads#L7>`_
and an exception of greater resource for the transfer processors. The values for 
these were arrived at by using the command::

    kubectl top pod -n {NLDS_NAMESPACE}

.. |sc| raw:: html

    <code class="code docutils literal notranslate">Ctrl + `</code>

within the kubectl shell on the appropriate rancher cluster (accessible via the 
shell button in the top right, or shortcut |sc|). ``{NLDS_NAMESPACE}`` will need 
to be replaced with the appropriate namespace for the cluster you are on, i.e.::

    kubectl top pod -n nlds                     # on wigbiorg
    kubectl top pod -n nlds-consumers-master    # for consumers on staging cluster
    kubectl top pod -n nlds-api-master          # for api-server on staging cluster

and, as before, these will likely need to be adjusted as understanding of the 
actual resource use of each of the microservices evolves. 

.. _chowning:

Changing ownership of files
---------------------------

A unique problem arose in beta testing where the NLDS was not able to change 
ownership of the files downloaded during a ``get`` to the user that requested them
from within a container that was not allowed to run as root. As such, a solution 
was required which allowed a very specific set of privileges to be escalated 
without leaving any security vulnerabilities open.

The solution found was to include an additional binary in the 
``Generic Consumer`` image - ``chown_nlds`` - which has the ``setuid`` 
permissions bit set and is therefore able to change directories. To minimise 
exposed attack surface, the binary was compiled from a `rust script <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/blob/master/images/consumer/chown_nlds.rs?ref_type=heads>`_ 
which allows only the ``chown``-ing of files owned by the NLDS user (on JASMIN 
``uid=7054096``). Additionally, the target must be a file or directory and the 
``uid`` being changed to must be greater than 1024 to avoid clashes with system 
``uid``s. This binary will only execute on any containers where the appropriate 
security context is set, notably::

    securityContext:
        allowPrivilegeEscalation: true
        add:
            - CHOWN

which in the NLDS deployment helm chart is only set for the ``Transfer-Get`` 
containers/pods.


.. _archive_put:

Archive Put Cronjob
-------------------

The process by which the archive process is started has been automated for this 
deployment, running as a `Kubernetes cronjob <https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/>`_ 
every 12 hours at midnight and midday. The Helm config controlling this can be 
seen `here <https://gitlab.ceda.ac.uk/cedadev/nlds-consumers-deploy/-/blob/master/conf/archive_put/common.yaml?ref_type=heads#L1-3>`_.
This cronjob will simply call the ``send_archive_next()`` entry point, which 
sends a message directly to the RabbitMQ exchange for routing to the Catalog. 


.. _staging:

Staging Deployment
------------------

As alluded to earlier, there are two versions of the NLDS running: (a) the 
production system on wigbiorg, and (b) the staging/beta testing system on the 
staging cluster (``ceda-k8s``). These have similar but slightly different 
configurations, the details of which are summarised in the below table. Like 
everything on this page, this was true at the time of writing (2024-03-06).


.. list-table:: Staging vs. Production Config
   :widths: 20 40 40
   :header-rows: 1

   * - System
     - Staging
     - Production
   * - Tape
     - Pre-production instance (``antares-preprod-fac.stfc.ac.uk``)
     - Pre-production instance (``antares-preprod-fac.stfc.ac.uk``)
   * - Database
     - on ``db5`` - ``nlds_{db_name}_staging``
     - on ``db5`` - ``nlds_{db_name}``
   * - Logging
     - To ``fluentbit`` with tags ``nlds_statging_{service_name}_log``
     - To ``fluentbit`` with tags ``nlds_prod_{service_name}_log``
   * - Object store
     - Uses the ``cedaproc-o`` tenancy 
     - Uses ``nlds-cache-02-o`` tenancy, ``nlds-cache-01-o`` also available
   * - API Server
     - `https://nlds-master.130.246.130.221.nip.io/ <https://nlds-master.130.246.130.221.nip.io/docs>`_ (firewalled)
     - `https://nlds.jasmin.ac.uk/ <https://nlds.jasmin.ac.uk/docs>`_ (public, ssl secured)


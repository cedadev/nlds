Retry reasons
=============

Retry types
-----------

* Decorator retry
* Message retry

* Transaction level retries
* File level retries

Retries in the code
-------------------

Going through the code, finding the reason(s) for retries.

API server
----------

* Decorator retries
    * None
* Message retries
    * None

NLDS
----

* Decorator retries
    * None
* Message retries
    * None

Indexer
-------

* Decorator retries
    * None
* Message retries
    * Path does not exist
        * Does it really not exist or is it Quobyte?
    * Path is not accessible to user (wrong read permissions, for example)
    * User id or group id not set / not found from OS
    * File does not exist
        * Does it really not exist or is it Quobyte?

Can it fail cleanly?  What happens on SIGHUP, SIGTERM?
[Link from Alex](https://github.com/orgs/cedadev/projects/90/views/1)

Catalog Worker
--------------

* Decorator retries
    * None
* Message retries
    * Catalog PUT
        * File already exists in Holding
        * File could not be added to the database
        * Location could not be added to the database
    * Catalog GET
        * Could not find file(s) belonging to (when user has requested them):
            * Holding_label
            * Holding_id
            * Transaction_id
            * Tag
            * Original_path
        * Location of storage type not found for file
        * Could not find file with original path
        * Could not find location for file (i.e. no OBJECT STORAGE or TAPE location in DB)
        * Could not find aggregation
    * Catalog DEL
        * Could not find file(s) belonging to:
            * Holding_label
            * Holding_id
            * Transaction_id
            * Tag
            * Original_path

Logger
-------

* Decorator retries
    * None
* Message retries
    * None

Monitor
-------

* Decorator retries
    * None
* Message retries
    * None

Transfer_put
------------

* Decorator retries
    * S3 error connecting to Object Storage
* Message retries
    * HTTP error on upload
    * Path is inaccessible (changed since indexing)

Transfer_get
------------

* Decorator retries
    * S3 error connecting to Object Storage
* Message retries
    * Path is inaccessible (changed since indexing)
    * Couldn't change owner of downloaded file
    * Couldn't change permissions of downloaded file
    * Download time exception occurred (undefined exception!)

Archive Worker (send_archive_next)
--------------

* Decorator retries
    *  None
* Message retries
    * None

Archive_put
-----------

* Decorator retries

* Message retries
    * HTTPError
    * S3Error
    * ArchiveError

Archive_get
-----------

* Decorator retries
    * S3Error, HTTPError in stream_tarmember
* Message retries
    * ArchiveError - could not unpack info from path_details
    * TarMemberError - could not get bucket_name from message info
    * HTTPError, S3Error in validate_bucket  - bucket doesn't exist or couldn't be made

New retry / error classifications:
--------------------------

* User errors - do not retry, fail straight away
    * File does not exist on disk
    * File cannot be read due to incorrect permissions
    * etc.
* System errors - retry
    * Tape not available
    * Disk not available
    * Object store not available
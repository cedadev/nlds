Clearing the Rabbit Queues
==========================

Sometimes an errant request can be created, usually if something goes wrong in development.
To delete requests from the queues:

1. Log into the `rbq1.ceda.ac.uk` server as root.  You will need to have your ssh key approved for root login to this machine.

2. List the contents of the queues for the `vhost` you are using:

    `rabbitmqctl list_queues --vhost <vhost_name>`

    1. For NLDS production, the vhost is `nlds_prod`
    2. For NLDS staging, the vhost is `nlds_staging`

    Example:

    `rabbitmqctl list_queues --vhost nlds_prod`

3. Clear a queue for the `vhost` and `queue`:

    `rabbitmqctl purge_queue <queue_name> --vhost <vhost_name>`

    Available queues:
    * logging_q
    * archive_put_q
    * transfer_get_q
    * monitor_q
    * archive_get_q
    * nlds_q
    * catalog_q
    * index_q
    * transfer_put_q

    Example:

    `rabbitmqctl purge_queue catalog_q --vhost nlds_prod`
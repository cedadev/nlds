Using System Status
===================


This is a way of checking which parts of the NLDS are currently online
and which are offline as well as the IDs of any offline consumers.

* A service is a part of the NLDS queues e.g: Monitor, Catalog, Index, etc..
* A consumer is part of the service there can be many consumers par service

This was created because there wasn't originally an easy way to view if any part of the NLDS was
offline.

This was made by using the RabbitMQ messaging system to send messages to individual
Service consumers to check if they reply or not. These messages only contain a distinction 
that it is being used for a test. If the consumer detects this distinction they will reply
and then stop so it doesn't confuse the consumer with a test message.

If they reply they will be marked as online,if they don't or are too slow they are 
considered to be offline and their unique tag is recorded and displayed on the table 
to easily determine what is working and what isn't.


This distinction is inside the msg_dict dictionary that is passed in the message::

    {
        "details": {
            "api_action": "system_stat", 
            "target_consumer": "", 
            "ignore_message": False
        }
    }

* api_action contains "system_stat" which is the identifier that tells the consumer that 
  it is a test message
* target_consumer starts as empty and is filled with the target service's name e.g: monitor
* ignore_message is by default False, this is used to test what happens if one of the consumers broke 
  it is automatically changed in the code if it is being tested

What is returned by the consumer is what is returned when a non system stat message is sent::

    publish_message(
        properties.reply_to,
        msg_dict=body,
        exchange={'name': ''},
        correlation_id=properties.correlation_id
    )

The only difference between this published message and a normal one is the meg_body 
this is because the body contains the details dictionary above distinguishing it from 
other messages


|

Running
-------


After the uvicorn server is running go to ```/system/stats/``` on a search engine
e.g: http://127.0.0.1:8000/system/stats/

This is the only step their is and there is nothing to configure.

To get the dictionary that is used to fill the table, it is possible to use requests.get 
as an API to directly get this information instead of using the web link.


adding ?time-limit={number} to the end of the URL will change the time limit 
(more on that below) e.g:
http://127.0.0.1:8000/system/stats/?time-limit=2

This is not necessary for the web page to work but it adds customisability.


|

Understanding the table
-----------------------


When opening the page it will load quickly unless some consumers have failed. 
This is because the system will wait the duration of the time limit set in system.py
the default for this is 5 seconds but can be changed by changing the value of time_limit. 
it can also be changed by adding ?time-limit={number} at the end of the URL. This 
number cannot go below 0 or above 15 otherwise it defaults to 5 seconds.

You will see a table with 3 columns as well as an info bar above
the info bar will give you a summary of the tables information.


1.  the left most table column holds all 7 NLDS services
2.  the middle column will say how many consumers in each service is running
    (and change colour depending on that number)
3.  the right most column will display the tag of any or all consumers that failed
    to be ran


One consumer tag links to an individual consumer for a service for example if you 
run ```nlds_q``` on 3 different terminals then you will have 3 consumers for the NLDS Worker
service each of these consumers will have their own tag that can be used to determine 
which (if any) have stopped working.


The table should look something like this (with examples of different status):
    =============  =========================================  =========================================
    Service        Status                                     Failed Consumer Tags (if any)
    =============  =========================================  =========================================
    Monitor        All Consumers Offline (None running)
    Catalog        All Consumers Online (3/3)
    NLDS Worker    Consumers Online (1/2)                     ctag1.732d21f82b4c47dcbd7dabe12f95315c
    Index          Login error
    Get Transfer   403 error
    Put Transfer   Rabbit error
    Logger         All Consumers Offline (0/2)                ctag1.732d21f82b4c47dcbd7dabe12f95315c
    Logger         (the ctag here will be on the row above)   ctag1.040535d3708c4012a4d2e6b0e6884cf2
    =============  =========================================  =========================================

The errors on Index, Get Transfer and Put Transfer are for illustrative purposes and are not accurate 
representations of what the whole table will look like.

|

**possible examples of how the system status table can look:**

No consumers are running. Blue info bar. All text is red.

.. image:: status_images/all_off.png
  :width: 400
  :alt: All consumers off
|
All consumers inside a service are offline. Red info bar and all failed tags in the row. 
the failed text is red, the rest is green.

.. image:: status_images/failed.png
  :width: 400
  :alt: A consumer failed
|
Some consumers inside a service are offline. Red info bar and all failed tags in the row. 
the partially failed service is in orange.

.. image:: status_images/part_failed.png
  :width: 400
  :alt: some consumers failed
|
All consumers online. Green info bar nothing in failed consumer column. all text in green.

.. image:: status_images/success.png
  :width: 400
  :alt: All consumers on

|

We get the number of consumers that should be online by using the requests.get API 
which returns a response containing a dictionary of all consumers in a specific service 
this is counted and used as the total consumers. 

|

responses
---------


What is returned to the HTML template is a dictionary that could be retrieved using an
API. This is its structure::

    {
        "monitor": monitor,
        "catalog": catalog,
        "nlds_worker": nlds_worker,
        "index": index,
        "get_transfer": get_transfer,
        "put_transfer": put_transfer,
        "logger": logger,
        "failed": failed_info
    }

Where the variables for the services will be::

    {
        "val": "Consumers Online 2/3", 
        "colour": "ORANGE", 
        "failed": consumers_fail
    }

* val = a string with how many consumers there are and how many are online
* colour = the colour that is used to colour the text in the HTML
* failed = a list of failed consumer tags (only exists if at least one consumer has failed)


Where the value of failed_info is::
    
    {
        "failed_num": num,
        "failed_colour": colour
    }

* num = the total number of failed consumers across all services
* colour = HTML string used to colour the INFO box

|

Errors
------


There may be some times when this page doesn't work properly.
This can include but is not limited to:

1.  The uvicorn server is not running (page will not load)
2.  The RabbitMQ server is down (the Status says ```Rabbit error```)
3.  The requests API has failed (the Status says ```403 error```)
4.  If you have put in invalid login information into .server_config
    (the Status says ```Login error```)
5.  If there is an unexpected error with the requests return then the code will
    catch it and show the json value of what was returned under the Status


If the RabbitMQ server is down, after it is back up then ```logging_q``` needs to be ran 
first in order for other services to work. Even if most of the RabbitMQ server is down, 
if only api_queues is down then the requests.get function will not be able to find 
the object and therefore a Rabbit error will occur. This is because it will return::
    
    {'error': 'Object Not Found', 'reason': 'Not Found'}

|

TLDR
----


going to ```/system/stats/``` on a search engine or http://127.0.0.1:8000/system/stats/
will show you a table of what services are currently running and the tags of any consumers 
that have failed
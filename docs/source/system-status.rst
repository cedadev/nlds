Using System Status
===================


This is a way of checking which parts of the NLDS are currently online
and which are offline as well as the IDs of any offline consumers



Running
-------


After the uvicorn server is running go to ```/system/stats/```
e.g: http://127.0.0.1:8000/system/stats/



Understanding
-------------


when opening the page it will load quickly unless some consumers have failed

you will see a table with 3 columns as well as an info bar above
the info bar will give you a summary of the tables information


1.  the left most table column holds all 7 NLDS services
2.  the middle column will say how many consumers in each service is running
    (and change colour depending on that number)
3.  the right most column will display the tag of any or all consumers that failed
    to be ran


One consumer tag links to an individual consumer for a service for example if you 
run ```nlds_q``` on 3 different terminals then you will have 3 consumers for the NLDS Worker
service each of these consumers will have their own tag that can be used to determin 
which (if any) have stoped working


The table should look something like this (with examples of different status):
    =============  =======================================  =========================================
    Service        Status                                   Failed Consumer Tags (if any)
    =============  =======================================  =========================================
    Monitor        All Consumers Offline (None running)
    Catalog        All Consumers Online (3/3)
    NLDS Worker    Consumers Online (1/2)                   ctag1.732d21f82b4c47dcbd7dabe12f95315c
    Index          All Consumers Online (3/3)
    Get Transfer   403 error
    Put Transfer   Rabbit error
    Logger         All Consumers Offline (0/2)              ctag1.732d21f82b4c47dcbd7dabe12f95315c
    Logger         (this will be on the same line)          ctag1.040535d3708c4012a4d2e6b0e6884cf2
    =============  =======================================  =========================================

the errors on this table will most likely cause the whole table to have the same
error this is just a representation


Errors
------


There may be some times when this page doesn't work properly.
This can include but is not limmited to:

1.  The uvicorn server is not running (page will not load)
2.  The rabbits server is down (the Status says ```Rabbit error```)
3.  The requests API has failed (the Status says ```403 error```)


If the Rabbit server is down, after it is back up then ```logging_q``` needs to be ran
first in order for other services to work
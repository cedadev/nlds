What do the consumers actually do?

Four different consumers defined:
* RabbitMQPublisher
* RabbitMQConsumer
* RabbitMQRPCPublisher
* StattingConsumer

RabbitMQPublisher (in publisher.py)
-----------------
* RabbitMQPublisher has all the Routing keys in! -> urgh, move to a enum or namespace (done - moved to new file / namespace)
* logger declared outside class (has to be for retry decorator)
* Server config has a load of string consts just in the file -> urgh, move to an enum or namespace (done - moved to new file / namespace)
* _default_logging_conf halfway down the class -> urgh, move up the class or to its own enum / namespace
* DEFAULT_RETRY_DELAYS in the class, move out? (done - moved to new file / namespace)
* Do the retry delays have to be defined in the publisher? (done - moved to new file / namespace)
* setup_logging too long
* Not sure what purpose of log and _log are?

What does PathDetails do / contain?

RabbitMQConsumer (in consumer.py)
----------------
* Server config import ugly 
* logger declared out in space (has to be, see above)
* What is the purpose of RabbitQEBinding(BaseModel) class? - (defines a binding)
* What is the purpose of RabbitQueue(BaseModel) class? - (contains list of bindings)
* FileListType and State - are they necessary? (State is, FileListType is part of retry mechanism)
* Move State to its own file (done)
* The failed / complete / retry lists could be a class / object
* TODO comment line 254 not relevant anymore? (load_config_value)
* Should we have a PathDetails list class with dedup operation in there?
* send_pathlist seems complicated, what is the logic?
* setup_logger - could the logger be its own class?
* change nack_message to nacknowledge_message to match naming convention above (done)
* EXPECTED_EXCEPTIONS declared in the middle of the class!
* _wrapped_callback - what is it actually doing?
* improve sigterm handler?

StattingConsumer (in statting_consumer.py)
----------------
* Adds functionality for statting files and checking file permissions - why does this have to be in a class?
* _choose_list - what is this, returns retrylist, completelist or failedlist?
* append_and_send - I really hate this, needs to be factored out into a retry list class
* set_ids - manipulates self.uid and self.gids, but where are these used?
* check path access - checks that a path is accessible on uid and gid - this could be moved to another class, or function.

KeepaliveDaemon (in keepalive.py)
---------------
* This seems neccessary to keep the connection up during long transactions

RabbitMQRPCPublisher (in rpc_publisher.py)
--------------------
* Handles remote procedure calls
* Doesn't do much and seems sensible so leave alone!

details.py
----------
* The yucky way the member variables are declared is due to pydantic!
* Determine what the purpose of each part of the class are actually for - there seems to be duplication

index.py
--------

catalog_worker.py
-----------------
* Change so that the location is created after the transfer is complete (_catalog_put)
* 

nlds_worker.py
--------------
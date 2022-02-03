Core content of the nlds-server
===============================

The Publisher class
-------------------

.. automodule:: nlds.rabbit.publisher
    :members:

    .. autoclass:: RabbitMQPublisher
        :members:
        :show-inheritance:

The routers
-----------
The routers control the endpoints of the API, and are the backbone of the 
nlds-server API. They are documented automatically (thanks to Swagger UI) and 
instructions on accessing these can be found in :doc:`home`.

The authenticators
------------------

.. automodule:: nlds.authenticators.base_authenticator
    :members:
    :undoc-members:

.. automodule:: nlds.authenticators.jasmin_authenticator
    :members:
    :undoc-members:

Authenicate methods also contains 3 general methods, used by the above 2 
modules, to validate the given user, group and token.

    
The server config module
------------------------

.. automodule:: nlds.server_config
    :members:
    :undoc-members:
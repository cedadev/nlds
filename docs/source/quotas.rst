NLDS quotas
================================

Get the quota for your group in the NLDS.


Implementation.
------------------------

This is currently only implemented for the `jasmin_authenticator`.

To get the quota, the `project_services_url` and `user_services_url` need to be present in the `jasmin_authenticator` section of the `server-config.rst` file.

First, there is a call to the JASMIN Projects Portal to get information about the service. This call is made to the `project_services_url`.
This is authorized on behalf of the NLDS using a client token, supplied in the config.
The tape quota is then extracted from the service information. Only quota for allocated tape resource in a Group Workspace is returned, no other categories or status of resource (such as pending requests) are included.
The quota command can be called from the `nlds client`. 
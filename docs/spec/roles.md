ROLES and QUOTAS in NLDS
========================

ROLES
-----

Users of NLDS can have three ROLES:

1. USER
2. DEPUTY
3. MANAGER

These are the same as the ROLES that a user can have for a group workspace on JASMIN.

For the NLDS, the roles allow the user these capabilities:

1. A USER can write data to a NLDS group (GWS), and read any data from that group, including data that was ingested by another user.  However, a USER can only DELETE their own data.
2. A DEPUTY has all the capabilities of the USER, but they can also DELETE any data from the group.
3. A MANAGER has the same capabilities as a DEPUTY, but they can also appoint DEPUTIES.  This is only useful in the JASMIN accounts and doesn't have any meaning in NLDS.  However, we still need to check for this role, as a MANAGER may not (probably will not) also have the DEPUTY role, and we need to give them the same capabilities as the DEPUTIES.

To get the information for a role, the JASMIN Accounts Portal needs to be contacted.  We may need to write a new endpoint.  Ideally we would have a single call that would return the roles that a user has for a group workspace.  However, as the group workspaces are defined as services, we may need to do more than one call.
Users have been authenticated and authorised for the group they are trying to perform a request for.  This uses OAuth2 Resource Owner Password Flow (which is deprecated but works really well for this case), with a `client-id` and `client-secret` which are contained in the `.nlds` user config file and instantiated on the `nlds init` call that the user does first.
However, for checking a ROLE, the NLDS will be acting on behalf of the user (who is already authenticated).  We can use the Client Credentials flow with a difference `client id` and `client secret` than that used by the Password Flow.  Varying the `client id` and `client secret` prevents any NLDS user, who has the `client id` and `client secret` in their `.nlds` config file from accessing the ROLES endpoint.

In the NLDS code, the checking for whether a user has permission to perform certain actions is done in the `nlds/nlds_processors/catalog.py` in the functions:

* `_user_has_get_file_permission`
* `_user_has_get_holding_permission`
* `_user_has_delete_from_holding_permission`

Currently, these functions just rely on the (authenticated) user and group passed in via the message - i.e. from the user request that has trickled through the system.  This is probably fine for the first two (`_user_has_get_file_permission` and `_user_has_get_holding_permission`), as it is just checking that the group in the *Holding* matches that in the message / request.

For the `_user_has_delete_from_holding_permission` we need to determine the user's ROLE, so that DEPUTIES and MANAGERS can delete other USER's data.  This will need to connect to the JASMIN accounts portal to get the user's ROLE.

An added subtlety is that NLDS is designed to be **portable**, so that it can be installed on other systems, rather than just JASMIN.  This was a condition of the EU Horizon ESiWACE funding.  To do this for the other authentication, I implemented a Base Class (`BaseAuthenticator` in `nlds/authenticators/base_authenticator.py`) which is then sub-classed to `JasminAuthenticator` in `nlds/authenticators/jasmin_authenticator.py`.  There are a number of functions that return a `True` or `False` value.  I suggest we add a function to the `BaseAuthenticator`, then overload it in `JasminAuthenticator` to provide a `True` or `False` value, depending on whether the user is a MANAGER or DEPUTY of the group.

Then in the call to `_user_has_delete_from_holding_permission` the `is_admin` parameter can be set from the return value of this function.

NLDS will need a `oauth_client_id` and `oauth_client_secret` to access the JASMIN accounts authenticator.  This should be different to the client id and secret handed out to the users, and will need to be stored in a config file on the NLDS FastAPI server.
I suggest putting it into the `/etc/nlds/server_config` in this section:
```
    "authentication" : {
        "authenticator_backend" : "jasmin_authenticator",
        "jasmin_authenticator" : {   
            "user_profile_url" : "https://accounts.jasmin.ac.uk/api/profile",
            "user_services_url" : "https://accounts.jasmin.ac.uk/api/services/",
            "oauth_token_introspect_url" : "https://accounts.jasmin.ac.uk/oauth/introspect/",
            "oauth_client_id" : { {oauth_client_id} },
            "oauth_client_secret" : { {oauth_client_secret} }
        },
```
The templated variables `oauth_client_id` and `oauth_client_secret` will need filling out by the deployment.  This is in the GitLab `nlds-deploy` repository.

The easiest way to test this will be to write a unit / integration test.  Test cases:
1. User belongs to GWS and has DEPUTY or MANAGER ROLE == True
2. User belongs to GWS and does not have DEPUTY or MANAGER ROLE == False
3. User does not belong to GWS == False
4. User does not belong to GWS but does have DEPUTY or MANAGER ROLE for a different GWS == False

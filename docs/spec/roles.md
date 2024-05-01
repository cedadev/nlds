ROLES and QUOTAS in NLDS
========================

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
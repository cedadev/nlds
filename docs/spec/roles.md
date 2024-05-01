ROLES and QUOTAS in NLDS
========================

Users of NLDS can have three roles:

1. USER
2. DEPUTY
3. MANAGER

These are the same as the roles that a user can have for a group workspace on JASMIN.

For the NLDS, the roles allow the user these capabilities:

1. A USER can write data to a NLDS group (GWS), and read any data from that group, including data that was ingested by another user.  However, a USER can only DELETE their own data.
2. A DEPUTY has all the capabilities of the USER, but they can also DELETE any data from the group.
3. A MANAGER has the same capabilities as a DEPUTY, but they can also appoint DEPUTIES.  This is only useful in the JASMIN accounts and doesn't have any meaning in NLDS.  However, we still need to check for this role, as a MANAGER may not (probably will not) also have the DEPUTY role, and we need to give them the same capabilities as the DEPUTIES.

To get the information for a role, the JASMIN Accounts Portal needs to be contacted.  We may need to write a new endpoint.  Ideally
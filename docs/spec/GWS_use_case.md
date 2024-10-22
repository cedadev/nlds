## Use cases

Showing version number when this feature will be available from a client a) on JASMIN b) from remote location

Feature | v JASMIN | v remote | notes
---|---|---|---
User Put|||
JASMIN USER copies a file from their GWS disk to the NLDS.                                                                  | 1.0 | 1.3 | 
JASMIN USER recursively copies a directory from their GWS disk to the NLDS.                                                 | 1.0 | 1.3 | 
JASMIN USER copies a specified set of files and/or directories to the NLDS by specifying an include file.                   | 1.0 | 1.3 | 
User Retrieve|||
JASMIN USER retrieves a file from the NLDS to their GWS disk.                                                               | 1.0 | 1.3 | 
JASMIN USER retrieves a directory from the NLDS to their GWS disk.                                                          | 1.0 | 1.3 | 
JASMIN USER retrieves a specified set of files and/or directories from the NLDS by specifying an include file.              | 1.0 | 1.3 | 
User Stage|||
JASMIN USER stages a file from the NLDS to the cache.                                                                       | 1.2 | 1.3 | Does this require a user-level cache quota, in addition to the GWS:cache quota?
JASMIN USER stages a directory from the NLDS to the cache.                                                                  | 1.2 | 1.3 | Can they set policy (residence time in cache?)
JASMIN USER stages a specified set of files and/or directories by specifying an include file.                               | 1.2 | 1.3 | 
JASMIN USER 2 in the same GWS as JASMIN USER 1, accesses files staged by JASMIN USER 1                                      | 1.2 | 1.3 |
File staged by a user are purged according to policy.
JASMIN USER accesses files in persistent staging location, for successive stage/unstage cycles                              | 1.2 | 1.3 | 
User Delete|||
JASMIN USER deletes a specified file from the NLDS.                                                                         | 1.1 |  | requires roles?
JASMIN USER recursively deletes a directory from the NLDS.                                                                  | 1.1 |  | requires roles?
JASMIN USER deletes a specified set of files and/or directories from the NLDS by specifying an include file.                | 1.1 |  | requires roles?
JASMIN USER copies a file from their GWS disk, and already present in the NLDS, as an incremental update to that file.      | 1.1 |  | requires roles?
User Misc|||
JASMIN USER retrieves a particular copy of a file from the NLDS to their GWS disk.                                          | ?   |  | Versions/copies?
JASMIN USER lists their holdings and files on the NLDS.  A holding is a collection of files (at the user view).             | 1.0 |  |
JASMIN USER monitors their transactions to/from NLDS.                                                                       | 1.1 |  | requires API for polling ingest status
JASMIN USER adds or amends metadata to a holding on the NLDS. e.g. tags, label.                                             | 1.0 |  |
JASMIN USER verifies their data (checksum).                                                                                 | 1.0 |  |
JASMIN USER specifies the GWS to which their PUT will be assigned, when PUTting to NLDS,                                    | 1.0 |  | requires GWS:tape, GWS:cache quotas, roles
JASMIN USER specifies the tape pool to which their PUT will be assigned, when PUTting to NLDS                               | 1.1 |  | requires tape pools
JASMIN USER checks usage against quotas for GWSs they belong to (2 quotas: GWS:tape, GWS:cache)                             | 1.1 |  | requires GWS:tape, GWS:cache quotas, roles
JASMIN USER cancels a command currently in progress                                                                         | 1.1 |  | 
Management funtions (for users with MANAGER or DEPUTY role on a GWS)|||
JASMIN MANAGER copies a file belonging to another user from their GWS to the NLDS.                                          | 1.0 |  |
JASMIN MANAGER deletes a file belonging to another from their GWS, from the NLDS.                                           | 1.0 |  |
JASMIN MANAGER (can do all the above actions for USER, for data belonging to another USER in the same GWS)                  | 1.0 |  |
Storing data on the tape requires quota remaining, allocated to GWS.  Quota is from the project portal.                     | 1.0 |  |

## Other

Types of quota:

- `GWS:tape` - quota for tape (aka archive) storage in the NLDS, per GWS.
- `GWS:cache` - quota for cache storage in the NLDS, per GWS.
- `USER:stage` - quota for staging within the cache, ...per GWS or per user?
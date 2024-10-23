## GWS Use cases

Showing version number when this feature will be available from a client a) on JASMIN b) from remote location

Feature | v JASMIN | v remote | notes
---|---|---|---
**User PUT**|||
JASMIN USER copies a file from their GWS disk to the NLDS.                                                                  | 1.0 | 1.3 | 
JASMIN USER recursively copies a directory from their GWS disk to the NLDS.                                                 | 1.0 | 1.3 | 
JASMIN USER copies a specified set of files and/or directories to the NLDS by specifying an include file.                   | 1.0 | 1.3 | 
JASMIN USER copies a file from their GWS disk already present in the NLDS, as an incremental update to that file.           | 1.0 | 1.3 | 
JASMIN USER specifies the GWS to which their PUT will be assigned, when PUTting to NLDS,                                    | 1.0 | 1.3 | quotas enforced in 1.1
**User GET**|||
JASMIN USER retrieves a file from the NLDS to their GWS disk.                                                               | 1.0 | 1.3 | 
JASMIN USER retrieves a directory from the NLDS to their GWS disk.                                                          | 1.0 | 1.3 | 
JASMIN USER retrieves a specified set of files and/or directories from the NLDS by specifying an include file.              | 1.0 | 1.3 | 
JASMIN USER retrieves data belonging to another user in the same GWS                                                        | 1.0 | 1.3 |
JASMIN USER retrieves a particular copy of a file identified by holding ID or label, from the NLDS to their GWS disk        | 1.0 | 1.3 | 
**User STAGE**|||
JASMIN USER stages a file from the NLDS to the cache.                                                                       | 1.2 | 1.3 | Does this require a user-level cache quota, in addition to the GWS:cache quota?
JASMIN USER stages a directory from the NLDS to the cache.                                                                  | 1.2 | 1.3 | Can they set policy (residence time in cache?)
JASMIN USER stages a specified set of files and/or directories by specifying an include file.                               | 1.2 | 1.3 | 
JASMIN USER 2 in the same GWS as JASMIN USER 1, accesses files staged by JASMIN USER 1                                      | 1.2 | 1.3 |
File staged by a user are purged according to policy.
JASMIN USER accesses files in persistent staging location, for successive stage/unstage cycles                              | 1.2 | 1.3 | 
**User DELETE**|||
JASMIN USER deletes a specified file from the NLDS.                                                                         | 1.1 | 1.3 | requires roles
JASMIN USER recursively deletes a directory from the NLDS.                                                                  | 1.1 | 1.3 | requires roles
JASMIN USER deletes a specified set of files and/or directories from the NLDS by specifying an include file.                | 1.1 | 1.3 | requires roles
**User Misc functions**|||
JASMIN USER lists their holdings and files on the NLDS.  A holding is a collection of files (at the user view).             | 1.0 | 1.3 |
JASMIN USER monitors their transactions to/from NLDS.                                                                       | 1.0 | 1.3 | requires API for polling ingest status
JASMIN USER adds or amends metadata to a holding on the NLDS. e.g. tags, label.                                             | 1.0 | 1.3 |
JASMIN USER verifies their data (checksum).                                                                                 | 1.1 | 1.3 |
JASMIN USER checks usage against quota for GWSs they belong to (GWS:tape)                                                   | 1.1 | 1.3 | requires GWS:tape quota
JASMIN USER cancels a command currently in progress                                                                         | 1.2 | 1.3 | 
**Management funtions** (for users with MANAGER or DEPUTY role on a GWS)|||
JASMIN MANAGER copies a file belonging to another user from their GWS to the NLDS.                                          | 1.0 | 1.3 | Requires read access to file in question, but that (MANAGER) user would then own that holding in NLDS.
JASMIN MANAGER deletes a file belonging to another USER from their GWS, from the NLDS.                                      | 1.1 | 1.3 | Requires DELETE and roles
JASMIN USER PUT has a default tape pool determined by contacting the projects portal to get the name of the default pool for that GWS | 1.1 | 1.3 | requires default tape pool as additional field on the GWS service in the PP

## Quotas

Types of quota:

- `GWS:tape` - quota for tape (aka archive) storage in the NLDS, per GWS. How much tape media the project is allowed to consume. Covers all holdings belinging to that GWS.
- Time-based policy for purging "cold" data from the cache. Possibility of preferentially purging data from **own** GWS data in cache, TBD.

### staging (TBD)
It **may** transpire that we need data staged for some minimum period, in which case an additional quota may be needed for this, at the GWS level:
- `GWS:cache` - quota for cache storage in the NLDS, per GWS.

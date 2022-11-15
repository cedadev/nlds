# Archive Backup replacement Use Case

Purpose: storage management for the CEDA Archive for tape and object store

Actors: CEDA Archive manager, data scientists setting policy, users retriving 

 - Data to tape/ob
    - Queue data to go to tape/object store via a policy set by data scientist. On deposit.  
    - Queue data to go to tape/object store on policy change.
    - Set policy for media

 - Maintaine data
    - Make archive copies - exact copy of what is in the archive for redundancy.
    - Tidy cache copies - partial copies on performance storage
    - Recovery copies - copies with deleted and modified (Backup)
    - Fixity Audit
    - Migration - copy and remove (could be archive, cache or recovery copies)
    - Space on media queries.

 - Access
    - Search
    - Request data to cache copy (NLA type behaviour)
    - Access (either for a download service or direct from mounted file system). (NLA Retrive data to disk (if only on tape or object store) and set retention on disk - data sscientists and users.)
    - Veiw status of backup system to make sure there are no problems and reassure data scientists that data is on the right media.




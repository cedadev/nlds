# Archive Backup replacement Use Case

Purpose: storage management for the CEDA Archive for tape and object store

Actors: CEDA Archive manager, data scientists setting policy, users retriving 

 - Data to archive tape/ob
 
    Make archive copies - exact copy of what is in the archive for redundancy 
    and Recovery copies - copies with deleted and modified (Backup)
    - Queue data to go to tape/object store via a policy set by data scientist as data is deposited.  
    - Queue data to go to tape/object store on policy change.
     
 - Maintain data
    - Set policy for media
    - Tidy cache copies - partial copies on performance storage
    - Fixity Audit
    - Migration - copy and remove (could be archive, cache or recovery copies)
    - Perge - remove data. Needs to be carefully controlled.
    - Report on Space on each media system.
    - Report on comformace to media policy.

 - Access
    - Search - list files. what am i searching on?
    - Browse - just look to see what is in the system. 
    - Request data to cache copy (NLA type behaviour)
    - Access - NLA style retrive data to disk (if only on tape or object store) with set retention on disk - data sscientists and users.
    - Veiw status of backup system to make sure there are no problems and reassure data scientists that data is on the right media.




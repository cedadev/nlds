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
    - Fixity Audit - compare checksums on tape, disk, object store with deposit checksums.
    - Migration - copy and remove (could be archive, cache or recovery copies)
    - Purge - remove data. Needs to be carefully controlled.
    - Report on Space on each media system.
    - Report on comformace to media policy.
    - Recovery - after a problem data is recovered to disk, tape or object store.

 - Access
    - Search - list files. what am i searching on?
    - Browse - just look to see what is in the system. 
    - Request data to cache copy (NLA type behaviour)
    - Access - NLA style retrive data to disk (if only on tape or object store) with set retention on disk - data sscientists and users.
    - Veiw status of backup system to make sure there are no problems and reassure data scientists that data is on the right media.



# Naratives

Sam has a new data set comming in. He know that it needs the standard media pattern, one disk, two tape copies. He looks to see if 
this is the default for his dataset. He confirms this and starts depositing the data. Its a biggy so and he needs the space in the arrivals area, so
he checks to see that the data is being sent to tape. Looks good so he feels he can remove the bits he has done from the arrivals area. This is 
much bigger than he thought.  He switches the policy to tape only, two tape copies. The data is verified that its on tape and then removed from disk.

Geraint wants half the data back on disk. He lists the files he would like and requests them. The data flows onto disk and 
signels when its been done. This is a tempary change that reverts to the tape only after the default time.

Wendy has a dataset that needs to be removed from all media. Sam, Ed and Wendy sign something in blood, the system records the event and removes the data.

Sam accidently removes lots of data from disk. The system notices, tells people and starts recovery. The recovery is checked against checksums. 

Ed wants to know if he should amend policy for sentinel. He checks how much space is on disk; not enough. He adjests the policy for this  






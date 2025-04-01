Making a XrootD key
===================

To read or write files to antares tape via XrootD, a key needs to be generated
and then passed to `xrdfs`, `xrdcp` or the Python libraries via two 
environment variables.

The key can be found in the `nlds-consumer-deploy` repository, in the
`conf/archive_put/staging_secrets.yaml` file (amongst others).  It is the
`tapeSecretSettings` Value.

Once you have this secret, create the keytab file from it and change the 
permissions (xrdfs will not let you use the key in any other state):

    echo "key string from repo" > ~/.nlds-xrd.keytab
    chmod g-r,o-r ~/.nlds-xrd.keytab

This will allow the user to use `xrdfs` and `xrdcp`.  To allow the nlds to use
tke key copy it to `/etc/nlds` and change ownership and permissions

    sudo cp ~/.nlds-xrd.keytab /etc/nlds/nlds-xrd.keytab
    sudo chown root /etc/nlds/nlds-xrd.keytab
    sudo chmod g-r,o-r /etc/nlds/nlds-xrd.keytab

Then set these environment variables:

    export XrdSecPROTOCOL=sss
    export XrdSecSSSKT=~/.nlds-xrd.keytab
CERN Tape Archive Set Up
========================

As part of the development of the NLDS, a tape emulator was set up to better 
understand how to interact with ``xrootd`` and the CERN tape archive. The 
following instructions detail how to set up such a tape emulator and are adapted 
from the instructions on [the CTA repo]
(https://gitlab.cern.ch/cta/CTA/-/tree/main/continuousintegration/buildtree_runner) 
and those provided by STFC's Scientific Computing Department at the Rutherford 
Appleton Laboratory. There are two major parts: commissioning the virtual 
machine, and setting it up appropriately according to the CERN instructions. 


Commissioning the VM
--------------------

These instructions are specifically for commissioning a VM on the STFC openstack
cloud interface. For other machines, slightly different instructions will need 
to be followed. 

After you have logged in to the openstack interface, you click on the "Launch 
Instance" button on the top to create the VM and then:

1. In the "Details" tab, give your VM a suitable name
2. In the "Source" tab, select scientificlinux-7-aq as a source image
3. In the "Flavour" tab, select a VM type depending on how many VCPUs, RAM and 
   disk size you need
4. In the "Networks" tab, select "Internal"
5. In the "Key Pair" tab, upload your public rsa ssh key so you can login to the 
   VM once it is created.
6. In the "Metadata" tab, click on the pull-down menu "Aquilon Image Properties" 
   and then set it to "Aquilon Archetype", specifying ``ral-tier1``, and also 
   "Aquilon Personality" specifying it as ``eoscta_ci_cd``. Note that you will 
   have to manually write these values out so it's worth copy pasting to avoid 
   typos!
7. Press the "Launch Instance" button and the VM will be created. Give it some 
   time so that quattor runs - quattor being a vm management tool like Puppet. 
   It may also need a reboot at some point.

This setup produces a vm which requires logging in as your openstack username - 
in most cases this will be your STFC federal ID. You will be able to sudo 
assuming the machine remains in the correct configuration.

**Note:**
The above setup is one that theoretically works. However, the machine which – 
after some attempts – successfully had CTA installed on it had to be 
commissioned manually by SCD so that  

(a) quattor could be made sure to have run successfully and then subsequently 
    disabled 
(b) I would be able to log in as ``root`` thus negating the need to edit the 
    sudoers file after each reboot. 

I would strongly recommend this approach if SCD are agreeable to commissioning 
your vm for you. 


Setting up CTA on the VM
------------------------

The following are the working set of instructions at time of writing, provided 
by SCD at RAL. 

* **Ensure quattor is not running**
  
  There should be an empty file at ``/etc/noquattor``, if there is not one then 
  create it with 

  ``sudo touch /etc/noquattor``

* **Clone the CTA gitlab repo**
 
  ``git clone https://gitlab.cern.ch/cta/CTA.git``
 
* **User environment**
 
  As per instructions
 
  ``cd ./CTA/continuousintegration/buildtree_runner/vmBootstrap``
 
  BUT in bootstrapSystem.sh, delete/comment line 46 and then
 
  ``./bootstrapSystem.sh``
 
  When prompted for password, press return (i.e don't give one). This creates 
  the ``cta`` user and adds them to sudoers
 
 
* **CTA build tree**
 
  As per instructions
 
  ``su - cta``
  ``cd ~/CTA/continuousintegration/buildtree_runner/vmBootstrap``
 
  BUT edit lines 54,55 in bootstrapCTA.sh to look like
 
  ``sudo wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle --no-check-certificate``
  ``sudo wget https://download.ceph.com/keys/release.asc -O /etc/pki/rpm-gpg/RPM-ASC-KEY-ceph --no-check-certificate``
 
  Note the change in the URL on line 55 from ``git.ceph.com`` to 
  ``download.ceph.com``, as well as the addition of the ``--no-check-certificate`` 
  flag. 
  
  Then run bootstrapCTA.sh (without any args)
 
  ``./bootstrapCTA.sh``
 
 
* **Install MHVTL**
 
  As per instructions
 
  ``cd ~/CTA/continuousintegration/buildtree_runner/vmBootstrap``
  ``./bootstrapMHVTL.sh``
 
* **Kubernetes setup**
 
  As per instructions
 
  ``cd ~/CTA/continuousintegration/buildtree_runner/vmBootstrap``
  ``./bootstrapKubernetes.sh``
 
  and reboot host

  ``sudo reboot``
 
* **Docker image**
 
  Depending on how your machine was set up you may now need to ensure that 
  quattor is still disabled (i.e. that the ``/etc/noquattor`` file still exists) 
  and that the cta user is still in the sudoers file. This will not be necessary 
  if you are running as ``root``.

  Then, as per instructions
 
  ``su - cta``
  ``cd ~/CTA/continuousintegration/buildtree_runner``
 
  BUT edit lines 38,39 in  /home/cta/CTA/continuousintegration/docker/ctafrontend/cc7/buildtree-stage1-rpms-public/Dockerfile to look like
 
  ``RUN wget https://public-yum.oracle.com/RPM-GPG-KEY-oracle-ol7 -O /etc/pki/rpm-gpg/RPM-GPG-KEY-oracle --no-check-certificate``
  ``RUN wget https://download.ceph.com/keys/release.asc -O /etc/pki/rpm-gpg/RPM-ASC-KEY-ceph --no-check-certificate``
 
  then run the master script to prepare all the Docker images. 
 
  ``./prepareImage.sh``
 
* **Preparing the environment (MHVTL, kubernetes volumes...)**
 
  As per instructions
 
  ``cd ~/CTA/continuousintegration/buildtree_runner``
  ``sudo ./recreate_buildtree_running_environment.sh``
 
* **Preparing the CTA instance**

  As per instructions
 
  ``cd ~/CTA/continuousintegration/orchestration``
  ``sudo ./create_instance.sh  -n cta -b ~ -B CTA-build -O -D -d internal_postgres.yaml``

  This may work first time but it never did for me, so the fix is to then run 

  ``./delete_instance.sh -n cta``

  To remove the instance and then re-create it with the same command as above
  ``sudo ./create_instance.sh  -n cta -b ~ -B CTA-build -O -D -d internal_postgres.yaml``

  This can be verified to be working with a call to 

  ``kubectl -n cta get pods``

  which should return a list of the working pods, looking something like:

    ============  ========  ========  =========  ===
    NAME          READY     STATUS    RESTARTS   AGE
    ============  ========  ========  =========  ===
    client        1/1       Running   0          35m
    ctacli        1/1       Running   0          35m
    ctaeos        1/1       Running   0          35m
    ctafrontend   1/1       Running   0          35m
    kdc           1/1       Running   0          35m
    postgres      1/1       Running   0          36m
    tpsrv01       2/2       Running   0          35m
    tpsrv02       2/2       Running   0          35m
    ============  ========  ========  =========  ===


Additional steps
----------------

The above steps will get you 90% of the way there but there's some extra steps 
required get `xrd-` commands running, and more for getting it running with 
pyxrootd. 

There're 3 main pods you'll want/need to use:
1. client - where you actually run the xrootd commands (as user1) and will 
eventually run the pyxrootd commands and the archiver
2. ctaeos - where you run the eos commands, which are basically filesystem admin 
commands, making directories, setting attributes of directories etc. 
3. ctacli - where you run cta-admin commands which, as far as I can tell, 
control the tape system metadata. 

For any of the above to work you'll first need to run the preparation script 
``/home/cta/CTA/continuousintegration/orchestration/tests/prepare_tests.sh`` 
to fill the cta database with the appropriate test metadata. This may or may not 
work straight up, but you may also need to run the command 

``kubectl --namespace cta exec ctafrontend -- cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 -m "docker cli"``

which recreates the ctaadmin1 user on the ctafrontend pod, a potentially 
necessary step after you've recreated the ctaeos instance - which you will 
probably have to to get it working (as per the "Preparing the CTA instance" 
section above). 

XRootD Python Bindings
----------------------

The python xrootd bindings are a bit of a pain to get to work on centos images, 
but after some (read as lots) of trial and error I managed to compile it using 
newer versions of a few packages and have baked it into a docker container. The 
image is on the [ceda registry under the NLDS consumers project]
(registry.ceda.ac.uk/nlds-consumers/archiver). The 
difficulty is getting these onto the `client` pod to be useful in developing 
against, but this was achieved by taking a snapshot image of the running client 
pod's container and then running a multi-stage build with both this image and 
the archiver image. [This image is now available on the registry]
(registry.ceda.ac.uk/nlds-consumers/ctaeos-client). You should be able to deploy 
this using the same pod.yaml that client uses, which you can steal by running:

``kubectl get pod -n cta client -o yaml``

and then save to a file, change out the image for your new image and then 
redeploy with 

``kubectl apply -f pod.yaml``

There are many ways to get images to the local repository so I will leave that 
as an exercise to the reader. 

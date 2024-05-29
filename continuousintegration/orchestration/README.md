# Launching a CTA test instance

A CTA test instance is a kubernetes namespace.
Basically it is a cluster of *pods* on its own DNS sub-domain, this means that inside a CTA namespace `ping ctafrontend` will ping *ctafrontend.<namespace>.cluster.local* i.e. the CTA frontend in the current instance, same for `ctaeos` and other services defined in the namespace.

Before going further, if you are completely new to `kubernetes`, you can have a look at this [CS3 workshop presentation](https://www.youtube.com/watch?v=0GMspkhavlM).
The web based presentation is available [here](http://jleduc.web.cern.ch/jleduc/mypresentations/170131_cs3_kubernetes-CTA.html).

## setting up the CTA `kubernetes` infrastructure

All the needed tools are self contained in the `CTA` repository.
It allows to version the system tests and all the required tools with the code being tested.
Therefore setting up the system test infrastructure only means to checkout `CTA` repository on a kubernetes cluster: a `ctadev` system for example.

## Everything in one go *aka* the **Big Shortcut**

This is basically the command that is run by the `gitlab CI` in the [CI pipeline](https://gitlab.cern.ch/cta/CTA/pipelines) executed at every commit during the `test` stage in the `archieveretrieve` build.
[Here](https://gitlab.cern.ch/cta/CTA/builds/258112) is an example of successfully executed `archieveretrieve` build.
Only one command is run in this build:
```
$ cd continuousintegration/orchestration/; \
./run_systemtest.sh -n ${NAMESPACE} -p ${CI_PIPELINE_ID} -s tests/archive_retrieve.sh
```

`CI_PIPELINE_ID` is not needed to run this command interactively: you can just launch:
```
[root@ctadevjulien CTA]# cd continuousintegration/orchestration/
[root@ctadevjulien orchestration]# ./run_systemtest.sh -n mynamespace -s tests/archive_retrieve.sh
```

But be careful: this command instantiate a CTA test instance, runs the tests and **immediately deletes it**.
If you want to keep it after the test script run is over, just add the `-k` flag to the command.

**By default, this command uses local VFS for the objectstore and local filesystem for an `sqlite` database,** you can add `-D` and `-O` flags to respectively use the central Oracle database account and the Ceph account associated to your system.

The following sections just explain what happens during the system test step and gives a few tricks and useful kubernetes commands.

## List existing test instances

This just means listing the current kubernetes namespaces:
```
[root@ctadevqa03 ~]# kubectl get namespace
NAME          STATUS    AGE
default       Active    18d
kube-system   Active    3d
```

Here we just have the 2 kubernetes *system* namespaces, and therefore no test instance.

## Create a `kubernetes` test instance

For example, to create `ctatest` CTA test instance, simply launch `./create_instance.sh` from `CTA/continuousintegration/orchestration` directory with your choice of arguments.
By default it will use a file based objectstore and an sqlite database, but you can use an Oracle database and/or Ceph based objectstore if you specify it in the command line.
```
[root@ctadevjulien CTA]# ./create_instance.sh
Usage: ./create_instance.sh -n <namespace> [-o <objectstore_configmap>] [-d <database_configmap>]
```

Objectstore configmap files and database configmap files are respectively available on `cta/dev/ci` hosts in `/opt/kubernetes/CTA/[database|objectstore]/config`, those directories are managed by Puppet and the accounts configured on your machine are yours.

**YOU ARE RESPONSIBLE FOR ENSURING THAT ONLY 1 INSTANCE USES 1 EXCLUSIVE REMOTE RESOURCE.
RUNNING 2 INSTANCES WITH THE SAME REMOTE RESOURCE WILL CREATE CONFLICTS IN THE WORKFLOWS AND IT WILL BE YOUR FAULT**

After all those WARNINGS, let's create a CTA test instance that uses **your** Oracle database and **your** Ceph objectstore.

```
[root@ctadevjulien CTA]# cd continuousintegration/orchestration/
[root@ctadevjulien orchestration]# git pull
Already up-to-date.
[root@ctadevjulien orchestration]# ./create_instance.sh -n ctatest \
 -o /opt/kubernetes/CTA/objectstore/config/objectstore-ceph-cta-julien.yaml \
 -d /opt/kubernetes/CTA/database/config/database-oracle-cta_devdb1.yaml -O -D
Creating instance for latest image built for 40369689 (highest PIPELINEID)
Creating instance using docker image with tag: 93924git40369689
DB content will be wiped
objectstore content will be wiped
Creating ctatest instance namespace "ctatest" created
configmap "init" created
creating configmaps in instance
configmap "objectstore-config" created
configmap "database-config" created
Requesting an unused MHVTL librarypersistentvolumeclaim "claimlibrary" created
.OK
configmap "library-config" created
Got library: sg35
Creating services in instance
service "ctacli" created
service "ctaeos" created
service "ctafrontend" created
service "kdc" created
Creating pods in instance
pod "init" created
Waiting for init.........................................................OK
Launching pods
pod "ctacli" created
pod "tpsrv" created
pod "ctaeos" created
pod "ctafrontend" created
pod "kdc" created
Waiting for other pods.....OK
Waiting for KDC to be configured..........................OK
Configuring KDC clients (frontend, cli...) OK
klist for ctacli:
Ticket cache: FILE:/tmp/krb5cc_0
Default principal: admin1@TEST.CTA

Valid starting     Expires            Service principal
03/07/17 23:21:49  03/08/17 23:21:49  krbtgt/TEST.CTA@TEST.CTA
Configuring cta SSS for ctafrontend access from ctaeos.....................OK
Waiting for EOS to be configured........OK
Instance ctatest successfully created:
NAME          READY     STATUS      RESTARTS   AGE
ctacli        1/1       Running     0          1m
ctaeos        1/1       Running     0          1m
ctafrontend   1/1       Running     0          1m
init          0/1       Completed   0          2m
kdc           1/1       Running     0          1m
tpsrv         2/2       Running     0          1m
```

This script starts by creating the `ctatest` namespace. It runs on the latest CTA docker image available in the gitlab registry. If there is no image available for the current commit it will fail. Then it creates the services in this namespace so that when the pods implementing those services are creates the network and DNS names are defined.

For convenience, we can export `NAMESPACE`, set to `ctatest` in this case, so that we can simply execute `kubectl` commands in our current instance with `kubectl --namespace=${NAMESPACE} ...`.

The last part is the pods creation in the namespace, it is performed in 2 steps:
1. run the `init` pod, which created db, objectstore and label tapes
2. launch the other pods that rely on the work of the `init` pod when its status is `Completed` which means that the init script exited correctly

Now the CTA instance is ready and the test can be launched.

Here are the various pods in our `ctatest` namespace:
```
[root@ctadevjulien orchestration]# kubectl get namespace
NAME          STATUS    AGE
ctatest       Active    4m
default       Active    88d
kube-system   Active    88d
[root@ctadevjulien orchestration]# kubectl get pods -a --namespace ctatest
NAME          READY     STATUS      RESTARTS   AGE
ctacli        1/1       Running     0          3m
ctaeos        1/1       Running     0          3m
ctafrontend   1/1       Running     0          3m
init          0/1       Completed   0          4m
kdc           1/1       Running     0          3m
tpsrv         2/2       Running     0          3m
```

Everything looks fine, you can even check the logs of `eos` mgm:
```
[root@ctadevjulien CTA]# kubectl --namespace $NAMESPACE logs ctaeos
...
### ctaeos mgm ready ###
```

Or the `Completed` init pod:
```
[root@ctadevjulien CTA]# kubectl --namespace $NAMESPACE logs init
...
Using this configuration for library:
Configuring mhvtl library
DRIVESLOT is not defined, using driveslot 0
export LIBRARYTYPE=mhvtl
export LIBRARYNAME=VLSTK60
export LIBRARYDEVICE=sg35
export DRIVENAMES=(VDSTK61 VDSTK62 VDSTK63)
export DRIVEDEVICES=(nst15 nst16 nst17)
export TAPES=(V06001 V06002 V06003 V06004 V06005 V06006 V06007)
export driveslot=0
Configuring objectstore:
Configuring ceph objectstore
Wiping objectstore
Rados objectstore rados://cta-julien@tapetest:cta-julien is not empty: deleting content
New object store path: rados://cta-julien@tapetest:cta-julien
Rados objectstore rados://cta-julien@tapetest:cta-julien content:
driveRegister-cta-objectstore-initialize-init-295-20170307-23:20:49-1
cta-objectstore-initialize-init-295-20170307-23:20:49
agentRegister-cta-objectstore-initialize-init-295-20170307-23:20:49-0
schedulerGlobalLock-cta-objectstore-initialize-init-295-20170307-23:20:49-2
root
Configuring database:
Configuring oracle database
Wiping database
Aborting: unlockSchema failed: executeNonQuery failed for SQL statement... ORA-00942: table or view does not exist
Aborting: schemaIsLocked failed: executeQuery failed for SQL statement ... ORA-00942: table or view does not exist
Aborting: schemaIsLocked failed: executeQuery failed for SQL statement ... ORA-00942: table or view does not exist
Database contains no tables. Assuming the schema has already been dropped.
Labelling tapes using the first drive in VLSTK60: VDSTK61 on /dev/nst15:
V06001 in slot 1 Loading media from Storage Element 1 into drive 0...done
1+0 records in
1+0 records out
80 bytes (80 B) copied, 0.00448803 s, 17.8 kB/s
Unloading drive 0 into Storage Element 1...done
OK
...
```

### Launching a cta-frontend-grpc pod 
An extra pod can be add to the cluster manually by running `kubectl create -f pod-ctafrontend-grpc.yaml --namespace=$NAMESCPACE` from the `continuousintegration/orchestration` directory.


# Running a simple test

Go in `CTA/continuousintegration/orchestration/tests` directory:
```
[root@ctadevjulien CTA]# cd continuousintegration/orchestration/tests
```

From there, launch `./systest.sh`.
It configures CTA instance for the tests and `xrdcp` a file to EOS instance, checks that it is on tape and recalls it:
```
[root@ctadevjulien tests]# ./systest.sh -n ctatest
Reading library configuration from tpsrvOK
Using this configuration for library:
 LIBRARYTYPE=mhvtl
 LIBRARYNAME=VLSTK60
 LIBRARYDEVICE=sg35
 DRIVENAMES=(VDSTK61 VDSTK62 VDSTK63)
 DRIVEDEVICES=(nst15 nst16 nst17)
 TAPES=(V06001 V06002 V06003 V06004 V06005 V06006 V06007)
 driveslot=0
Preparing CTA for tests
Drive VDSTK61 set UP.
EOS server version is used:
eos-server-4.1.24-20170616132055git722df4d.el7.x86_64
xrdcp /etc/group root://localhost//eos/ctaeos/cta/4ff9d278-4c92-427a-b72b-a28cefddffcd
[420B/420B][100%][==================================================][420B/s]  
Waiting for file to be archived to tape: Seconds passed = 0
...

OK: all tests passed
```

If something goes wrong, please check the logs from the various containers running on the pods that were defined during dev meetings:
1. `init`
  * Initializes the system, e.g. labels tapes using cat, creates/wipes the catalog database, creates/wipes the CTA object store and makes sure all drives are empty and tapes are back in their home slots.
2. `kdc`
  * Runs the KDC server for authenticating EOS end-users and CTA tape operators.
3. `eoscli`
  * Hosts the command-line tools to be used by an end-user of EOS, namely xrdcp, xrdfs and eos.
4. `ctaeos`
  * One EOS mgm.
  * One EOS fst.
  * One EOS mq.
  * The cta command-line tool to be run by the EOS workflow engine to communicate with the CTA front end.
  * The EOS Simple Shared Secret (SSS) to be used by the EOS mgm and EOS fst to authenticate each other.
  * The CTA SSS to be used by the cta command-line tool to authenticate itself and therefore the EOS instance with the CTA front end.
  * The tape server SSS to be used by the EOS mgm to authenticate file transfer requests from the tape servers.
5. `ctacli`
  * The cta command-line tool to be used by tape operators.
  * This pod has the keytab of `ctaadmin1` who is allowed to type `cta` admin commands.
6. `ctafrontend`
  * One CTA front-end.
  * The CTA SSS of the EOS instance that will be used by the CTA front end to authenticate the cta command-line run by the workflow engine of the EOS instance.
7. `tpsrvXX` *No two pods in the same namespace can have the same name, hence each tpsrv pod will be numbered differently*
  * One `cta-taped` daemon running in `taped` container of `tpsrvxxx` pod.
  * One `rmcd` daemon running in `rmcd` container of `tpsrvxxx` pod.
  * The tape server SSS to be used by cta-taped to authenticate its file transfer requests with the EOS mgm (all tape servers will use the same SSS).
8. `client`
  * The `cta` command-line tool to be used by `eosusers`.
  * The `eos` command-line tool.
  * This pod has the keytab of `user1` who is allowed to read-write file in `root://ctaeos//eos/ctaeos/cta`.


# post-mortem analysis

## logs

An interesting feature is to collect the logs from the various processes running in containers on the various pods.
Kubernetes allows to collect `stdout` and `stderr` from any container and add time stamps to ease post-mortem analysis.

Those are the logs of the `init` pod first container:
```
[root@ctadev01 CTA]# kubectl logs init --timestamps  --namespace $NAMESPACE
2016-11-08T20:00:28.154608000Z New object store path: file:///shared/test/objectstore
2016-11-08T20:00:31.490246000Z Loading media from Storage Element 1 into drive 0...done
2016-11-08T20:00:32.712066000Z 1+0 records in
2016-11-08T20:00:32.712247000Z 1+0 records out
2016-11-08T20:00:32.712373000Z 80 bytes (80 B) copied, 0.221267 s, 0.4 kB/s
2016-11-08T20:00:32.733046000Z Unloading drive 0 into Storage Element 1...done
```

## Dump the objectstore content

Connect to a pod where the objectstore is configured, like `ctafrontend`:
```
[root@ctadevjulien CTA]# kubectl --namespace $NAMESPACE exec ctafrontend -ti bash
```

From there, you need to configure the objecstore environment variables, sourcing `/tmp/objectstore-rc.sh` install the missing `protocolbuffer` tools like `protoc` binary, and then dump all the objects you want:
```
[root@ctafrontend /]# yum install -y  protobuf-compiler
...
Installed:
  protobuf-compiler.x86_64 0:2.5.0-7.el7                                        

Complete!

[root@ctafrontend /]# . /tmp/objectstore-rc.sh

[root@ctafrontend /]# rados ls -p $OBJECTSTOREPOOL --id $OBJECTSTOREID --namespace $OBJECTSTORENAMESPACE
OStoreDBFactory-tpsrv-340-20161216-14:17:51
OStoreDBFactory-ctafrontend-188-20161216-14:15:35
schedulerGlobalLock-makeMinimalVFS-init-624-20161216-14:14:17-2
driveRegister-makeMinimalVFS-init-624-20161216-14:14:17-1
makeMinimalVFS-init-624-20161216-14:14:17
agentRegister-makeMinimalVFS-init-624-20161216-14:14:17-0
root
RetrieveQueue-OStoreDBFactory-ctafrontend-188-20161216-14:15:35-3

[root@ctafrontend /]# rados get -p $OBJECTSTOREPOOL --id $OBJECTSTOREID \
 --namespace $OBJECTSTORENAMESPACE RetrieveQueue-OStoreDBFactory-ctafrontend-188-20161216-14:15:35-3 \
 - | protoc --decode_raw
1: 10
2: 0
3: "root"
4: "root"
5 {
  10100: "V02001"
  10131 {
    9301: 1
    9302: 1
  }
  10132 {
    9301: 1
    9302: 1
  }
  10133 {
    9301: 1
    9302: 1
  }
  10140: 406
  10150: 0
}
```

## Delete a `kubernetes` test instance

Deletion manages the removal of the infrastructure that had been created and populated during the creation procedure: it deletes the database content and drop the schema, remove all the objects in the objectstore independently of the type of resources (ceph objectstore, file based objectstore, oracle database, sqlite database...).

In our example:
```
[root@ctadevjulien CTA]# ./delete_instance.sh -n $NAMESPACE
Deleting ctatest instance
Unlock cta catalog DB
Delete cta catalog DB for instance
Deleted 1 admin users
Deleted 1 admin hosts
Deleted 1 archive routes
Deleted 1 requester mount-rules
Deleted 0 requester-group mount-rules
Deleted 1 archive files and their associated tape copies
Deleted 7 tapes
Deleted 1 storage classes
Deleted 1 tape pools
Deleted 1 logical libraries
Deleted 1 mount policies
Status cta catalog DB for instance
UNLOCKED
Drop cta catalog DB schema for instance
namespace "ctatest" deleted
................................OK
Deleting PV sg34 with status Released
persistentvolume "sg34" deleted
persistentvolume "sg34" created
OK
Status of library pool after test:
NAME      CAPACITY   ACCESSMODES   STATUS      CLAIM     REASON    AGE
sg30      1Mi        RWO           Available                       19h
sg31      1Mi        RWO           Available                       19h
sg32      1Mi        RWO           Available                       19h
sg33      1Mi        RWO           Available                       18h
sg34      1Mi        RWO           Available                       0s
sg35      1Mi        RWO           Available                       19h
sg36      1Mi        RWO           Available                       19h
sg37      1Mi        RWO           Available                       19h
sg38      1Mi        RWO           Available                       19h
sg39      1Mi        RWO           Available                       18h
```

When this deletion script is finished, the `ctatest` instance is gone: feel free to start another one...

```
[root@ctadevjulien CTA]# kubectl get namespace
NAME          STATUS    AGE
default       Active    6d
kube-system   Active    6d
```


## Long running tests

If some tests run for long, the kerberos token in the cli pod should be renewed with:

```
kubectl --namespace=${instance} exec ctacli -- kinit -kt /root/admin1.keytab admin1@TEST.CTA
```

# Gitlab CI integration

Configure the Runners for `cta` project and add some specific tags for tape library related jobs. I chose `mhvtl` and `kubernetes` for ctadev runners.
This allows to use those specific runners for CTA tape library specific tests, while others can use shared runners.

A small issue: by default, `gitlab-runner` service runs as `gitlab-runner` user, which makes it impossible to run some tests as `root` inside the pods has not the privileges to run all the commands needed.

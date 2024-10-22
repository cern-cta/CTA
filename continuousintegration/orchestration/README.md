# CTA Orchestration

In this directory you can find all the necessary files to deploy CTA test instance on a (pre-configured) Kubernetes cluster.
If you are reading this, then you are most likely interesting in setting up a local development environment. This is done via the [CTA CI Minikube](https://gitlab.cern.ch/cta/minikube_cta_ci) repository. This setup does four important things:

- It install and configures MHVTL (virtual tape library)
- It installs and configures Minikube (for running a local cluster)
- It sets up a number of persistent volumes. Specifically claimlogs (for the logs) and claimstg (for the EOS FSTs).
- Finally, it allows you to set up the necessary secrets to pull from the private registry.

Note that we are working on improving this setup and making it more portable. For specific details on how to get a local instance up and running, see [the development setup docs](https://eoscta.docs.cern.ch/latest/dev/development_setup/).

At the heart of the orchestration are two scripts:
- `create_instance.sh`: this script is responsible for creating the namespace and spawning all the pods. By the time this script has finished, a usable local CTA instance has been deployed.
- `delete_instance.sh`: at its simplest, this script simply deletes a given namespace. However, before doing so, it is able to collect logs and additional debug output. This is particularly useful in CI pipelines where all of these logs are stored as artifacts.

In addition to this, there is the `run_systemtest.sh` script. This script will do everything in one go. It will:
1. Cleanup/delete any old namespaces
2. Create a new CTA instance
3. Perform a setup script on this instance (e.g. intantiating some admin accounts, adding VOs, labeling tapes, etc)
4. Execute a provided test script (e.g. `tests/test_client.sh`).
5. Clean up the namespace again

Of course all of the above is configurable to a high degree; see the `--help` output of these scripts for additional details.

## How an instance is spawned

A CTA instance is fully deployed using [Helm](https://helm.sh/). Helm uses the concept of charts. To quote: "Helm uses a packaging format called charts. A chart is a collection of files that describe a related set of Kubernetes resources.". Helm makes it simpler for us to define a configurable complex Kubernetes application such as CTA. The configuration of the cluster is done through `values.yaml` files. Provided that the `values.yaml` has the expected structure (verified through the corresponding `schema.version.json`), we can pass in any configuration we want. The default `values.yaml` file provided in each corresponding chart will already have *most* of the required values set correctly. However, there are a few things that need to be provided manually, as they will either change frequently or are specific to your own setup.

To understand each of these required configuration options, we will go through the spawning of each component separately, detail how it works and what configuration is expected from you. Note that you most likely won't have to interact with Helm directly; all of the important/required configuration can be done through the `create_instance.sh` script.

Charts that have CTA-specific functionality (basically all of them), rely on an image `ctageneric`. This is the image built as specified in the `docker/` directory. This image contains all of the CTA RPMs, in addition to the startup-scripts required for each container.

### Init

The first chart that will be installed is the `init` chart. This chart sets up three important resources:
- A persistent volume claim for the logs. The reason is that each pod in the namespace writes their log to the same persistent volume (so that they can all be collected in one place).
- The Key Distribution Center (kdc) pod. This sets up everything necessary for authentication (both sss and kerberos). Runs the KDC server for authenticating EOS end-users and CTA tape operators.
- If configured, it will spawn a job that cleans up/wipes MHVTL. It does so by making sure that there are no tapes remaining in the drives.

The `init` chart expects the following required parameters:
- `global.image.tag`: the tag for the `ctageneric` image to use. This is used by the `kdc` pod.
- `global.image.registry`: while not technically required, it is typically desirable to provide this, as it will default to `gitlab-registry.cern.ch/cta` otherwise.
- `tapeConfig`: this contains the library configuration. This details which libraries are available, which tapes, which drives etc. This configuration is used by the MHVTL cleanup job.

The library configuration is the first of three "main" configurations that determine the overall behaviour of the instance. The library configuration can be provided explicitly to `create_instance.sh` using `--library-config <config-file>`. Alternatively, if this file is not provided, the script will auto-generate one based on the (emulated) hardware it finds using `lsscsi` commands. Such a configuration looks as follows:

```yaml
library:
  type: "mhvtl"
  name: "VLSTK10"
  device: "sg0"
  drivenames:
    - "VDSTK01"
    - "VDSTK02"
    - "VDSTK03"
  drivedevices:
    - "nst2"
    - "nst0"
    - "nst1"
  tapes:
    - "V00101"
    - "V00102"
    - "V00103"
    - "V00104"
    - "V00105"
    - "V00106"
    - "V00107"
    - "V00108"
```

It is important to note that - for now - the CTA instance setup only supports a single library (device). Each Helm deployment of CTA will get an annotation to specify which library it is using. When spawning a new CTA instance, it will first check if there are libraries available by looking at all the available libraries and looking at what is deployed. If a config file is provided with a library that is already in use, the instance spawning will fail.

### Catalogue

The `catalogue` chart is installed second and does a few things:
- First and foremost, it will create a configmap with the database connection string, i.e. `cta-catalogue.conf`.
- If configured, it will spawn a job that wipes the catalogue database (i.e. the one defined in `cta-catalogue.conf`).
- If Postgres is used as a catalogue backend, it will spawn a local Postgres database.

The catalogue supports both Oracle and Postgres backends. A Postgres database can be deployed locally, but an Oracle database cannot. As such, **when using Oracle it will use a centralized database**. This is of course not ideal, but there is no way around this. For development purposes, you are expected to use your own Oracle account (see [internal docs](https://tapeoperations.docs.cern.ch/dev/centrally_managed_resources/#oracle-dbs-for-development-and-ci)). This is also something extremely important to be aware of for the CI. It is vitally important that there is only ever a single instance connecting with a given account. Multiple instances connecting with the same account **will interfere with eachother**. To prevent this, each of our custom CI runners has their own Oracle account and jobs are not allowed to run concurrently on the same runner.

For the sake of repeatable tests, it is therefore important to always wipe the catalogue. Not doing so will cause the database to have potential leftovers from previous runs. It should also be noted, that at the end of the wipe catalogue job, the catalogue will be re-initialised with the configured catalogue schema. That means even an empty database will need to be "wiped" in order to initialise the catalogue. As such, the only situation in which one would not want to wipe the catalogue is if there is important data in the catalogue that you want to test on.

The `catalogue` chart expects the following required parameters:
- `wipeImage.tag`: the tag for the `ctageneric` image to use. Used by the wipe catalogue job.
- `wipeImage.registry`: while not technically required, it is typically desirable to provide this, as it will default to `gitlab-registry.cern.ch/cta` otherwise.
- `schemaVersion`: the catalogue schema version to deploy.
- `wipeCatalogue`: whether to wipe the catalogue or not.
- `configuration`: the catalogue configuration.

The catalogue configuration is the second of the three "main" configurations. It can be explicitly provided using the `--catalogue-config` flag. If not provided, it will default to `presets/dev-postgres-catalogue-values.yaml`. The configuration looks as follows:

```yaml
backend: "" # Can be sqlite, orale or postgres.
oracleConfig:
  username: ""
  database: ""
  password: ""
postgresConfig:
  username: ""
  password: ""
  server: ""
  database: ""
sqliteConfig:
  filepath: ""
```

Note that only one of these `*Config` fields needs to be provided (based on the backend).

### Scheduler

The `scheduler` chart can also be installed as soon as the `init` chart is ready. At the moment, to keep the script simple this done sequentially, but it could in theory be installed at the same time as the `catalogue` chart. The `scheduler` chart:
- Generates a a configmap containing `cta-objectstore-tools.conf`.
- If CEPH is the configured backend, it will create an additional configmap with some CEPH configuration details
- If configured, spawns a job that wipes the scheduler.

The `scheduler` chart expects the following required parameters:
- `wipeImage.tag`: the tag for the `ctageneric` image to use. Used by the wipe scheduler job.
- `wipeImage.registry`: while not technically required, it is typically desirable to provide this, as it will default to `gitlab-registry.cern.ch/cta` otherwise.
- `wipeScheduler`: whether to wipe the scheduler or not.
- `configuration`: the scheduler configuration.

The `scheduler` can be configured to use one of three backends: CEPH, VFS (virtual file system), or Postgres. This is configured through the scheduler configuration: the third and final of the three "main" configurations. It can be explicitly provided using the `--scheduler-config` flag. If not provided, it will default to `presets/dev-file-scheduler-values.yaml`. The configuration looks as follows:

```yaml
backend: "" # Options: file, ceph, postgres
cephConfig:
  mon: ""
  monport: ""
  pool: ""
  namespace: ""
  id: ""
  key: ""
postgresConfig:
  username: ""
  password: ""
  database: ""
  server: ""
fileConfig:
  path: ""
```

Note that only one of these `*Config` fields needs to be provided (based on the backend).

### CTA

Finally, we have the `cta` chart. This chart spawns the different components required to get a running instance of CTA:

- `ctaeos`
  * One EOS mgm.
  * One EOS fst.
  * One EOS mq.
  * The EOS Simple Shared Secret (SSS) to be used by the EOS mgm and EOS fst to authenticate each other.
  * The CTA SSS to be used by the cta command-line tool to authenticate itself and therefore the EOS instance with the CTA front end.
  * The tape server SSS to be used by the EOS mgm to authenticate file transfer requests from the tape servers.
- `ctacli`
  * The cta command-line tool to be used by tape operators.
  * This pod has the keytab of `ctaadmin1` who is allowed to type `cta` admin commands.
- `ctafrontend`
  * One CTA front-end.
  * The CTA SSS of the EOS instance that will be used by the CTA front end to authenticate the cta command-line run by the workflow engine of the EOS instance.
- `tpsrvXX` *No two pods in the same namespace can have the same name, hence each tpsrv pod will be numbered differently*
  * One `cta-taped` daemon running in `taped` container of `tpsrvxxx` pod.
  * One `rmcd` daemon running in `rmcd` container of `tpsrvxxx` pod.
  * The tape server SSS to be used by cta-taped to authenticate its file transfer requests with the EOS mgm (all tape servers will use the same SSS).
- `client`
  * The `cta` command-line tool to be used by `eosusers`.
  * The `eos` command-line tool.
  * This pod has the keytab of `user1` who is allowed to read-write file in `root://ctaeos//eos/ctaeos/cta`.

The `cta` chart expects the following required parameters:
- `global.image.tag`: the tag for the `ctageneric` image to use. Used by all the pods in the subcharts.
- `global.image.registry`: while not technically required, it is typically desirable to provide `global.image.registry`, as this will default to `gitlab-registry.cern.ch/cta` otherwise.
- `tapeConfig`: this is the library configuration. This details which libraries are available, which tapes, which drives etc. This configuration is used by the MHVTL cleanup job.
- `global.useSystemd`: whether to use systemd or not. For now, systemd support has not been implemented.
- `global.catalogueSchemaVersion`: The schema version of the catalogue. This is not currently used, but once all the charts use Kubernetes deployments, this can be used to automatically redeploy the relevant pods when this version changes (the frontend and tape servers).
- `global.configuration.scheduler`: The scheduler configuration (same as detailed above). This is required as some pods need to mount specific volumes to specific places when CEPH is used as a backend.
- `tpsrv.tpsrv.numTapeServers`: The number of tape servers to spawn. This must be less than or equal to the number of drives available. Each tape server will map itself to a drive according to its index (i.e. `tpsrv01` will get drive 0, as it is the "first" tape server). At some point this should be improved to be more configurable.
- `tpsrv.tapeConfig`: The library configuration (same as detailed above). Used by the tape servers.

### The whole process

To summarise, the `create_instance.sh` script does the following:

1. Generate a library configuration if not provided.
2. Check whether the requested library is not in use.
3. Install the `init` chart, which cleans MHVTL, sets up the common storage space for the logs and sets up the authentication centre.
4. Install the `catalogue` chart, which produces a configmap containing `cta-catalogue.conf` and spawns a job that wipes the catalogue. If Postgres is the provided backend, will also spawn a local Postgres DB.
5. Install the `scheduler` chart, generating a configmap `cta-objectstore-tools.conf` and spawning a job to wipe the scheduler.
6. Install the `cta` chart, spawning all the different CTA pods: a number of tape servers, an EOS MGM, a frontend, a client to communicate with the frontend, and an admin client (`ctacli`).
7. Perform some simple initialization of the EOS workflow rules and kerberos tickets on the client/ctacli pods.

Note that once this is done, the instance is still relatively barebones. For example, you won't be able to execute any `cta-admin` commands on the `ctacli` yet. To get something to play with, you are advised to run `tests/prepare_tests.sh`, which will setup some basic resources.

## Deleting a CTA instance

The deletion of an instance is relatively straightforward and can be done through the `delete_instance.sh` script. At its simplest, it just deletes the namespace. However, this script has some extra features to collect logs from any of the pods it is about to delete. Note that this deletion script does not clean up any resources outside of the name. That means that it will not perform any clean up on centralized databases or do any unloading of tapes still in the drives. This is all expected to be done BEFORE tests start.

## Some example commands

- Creating a test instance from a local build:

  ```sh
  # Note that build_deploy.sh resides in ci_runner/
  ./build-deploy.sh
  ```
- Redeploying a local build:
  ```sh
  # Note that build_deploy.sh resides in ci_runner/
  ./build-deploy.sh --skip-build --skip-image-reload
  ```
- Spawning a test instance from a tagged `ctageneric` image in the `gitlab-registry.cern.ch/cta` registry (Postgres catalogue + VFS scheduler):

  ```sh
  ./create_instance.sh -n dev --image-tag <some-tag>
  ```

- Running a system test locally from a tagged `ctageneric` image in the `gitlab-registry.cern.ch/cta` registry (Postgres catalogue + VFS scheduler):

  ```sh
  ./run_systemtest.sh -n dev --test-script tests/test_client.sh --scheduler-config presets/dev-file-scheduler-values.yaml --catalogue-config presets/dev-postgres-catalogue-values.yaml --image-tag <some-tag>
  ```

- Running a system test locally from a local image (Postgres catalogue + VFS scheduler):

  ```sh
  ./run_systemtest.sh -n dev --test-script tests/test_client.sh --scheduler-config presets/dev-file-scheduler-values.yaml --catalogue-config presets/dev-postgres-catalogue-values.yaml --image-tag <some-tag> --registry-host localhost
  ```

Of course, once an instance is spawned, you can also run some simple tests manually instead of relying on `run_systemtest.sh`:

```sh
./tests/prepare_tests.sh -n dev
./tests/test_client.sh -n dev
```

## Troubleshooting

When something goes wrong, start by looking at the logs:
- If the pod did not start correctly, run `kubectl describe pod <pod> -n <namespace>` to get information on why it is not starting
- Run `kubectl logs <pod> -c <container> -n <namespace>` to get the logs of a given container in a pod (`-c is optional if there is only one container).
- Run `kubectl exec -it -n <namespace> <pod> -c <container> -- bash`. This will start an interactive shell session in the given container. You can use that to inspect the logs in e.g. `/var/log/`.

### Dump the objectstore content

Connect to a pod where the objectstore is configured, like `ctafrontend`:

```sh
kubectl --namespace $NAMESPACE exec ctafrontend -it bash
```

From there, you need to install the missing `protocolbuffer` tools like `protoc` binary, and then dump all the objects you want.

```sh
[root@ctafrontend /]# yum install -y  protobuf-compiler
...
Installed:
  protobuf-compiler.x86_64 0:2.5.0-7.el7                                        

Complete!

[root@ctafrontend /]# rados ls -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID --namespace $SCHEDULER_CEPH_NAMESPACE
OStoreDBFactory-tpsrv-340-20161216-14:17:51
OStoreDBFactory-ctafrontend-188-20161216-14:15:35
schedulerGlobalLock-makeMinimalVFS-init-624-20161216-14:14:17-2
driveRegister-makeMinimalVFS-init-624-20161216-14:14:17-1
makeMinimalVFS-init-624-20161216-14:14:17
agentRegister-makeMinimalVFS-init-624-20161216-14:14:17-0
root
RetrieveQueue-OStoreDBFactory-ctafrontend-188-20161216-14:15:35-3

[root@ctafrontend /]# rados get -p $SCHEDULER_CEPH_POOL --id $SCHEDULER_CEPH_ID \
 --namespace $SCHEDULER_CEPH_NAMESPACE RetrieveQueue-OStoreDBFactory-ctafrontend-188-20161216-14:15:35-3 \
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

### Long running tests

If some tests run for long, the kerberos token in the cli pod should be renewed with:

```sh
kubectl --namespace=${namespace} exec ctacli -- kinit -kt /root/admin1.keytab admin1@TEST.CTA
```

## Gitlab CI integration

There are a number of custom runners in the `CTA` project on GitLab. Any job that has the tags `mhvtl` or `kubernetes` will go to these custom runners. All other jobs run on the shared runners.
A small issue: by default, `gitlab-runner` service runs as `gitlab-runner` user, which makes it impossible to run some tests as `root` inside the pods has not the privileges to run all the commands needed.

## Limitations

The current deployment of this CTA has a few limitations that make it unsuitable for a wider adoption. These limitations are listed below. Note that the list is not necessarily conclusive.

- Only a single replica of each pod can be deployed (apart from the tape servers), because each chart still uses plain pod configurations. This should be moved to deployments at some point.
- While it is possible to spawn multiple tape servers, it is not yet possible to easily configure which tape server should be responsible for which drive. By extension, it is also not yet possible to assign multiple drives to a tape server.
- It is not possible to define different schedulers for different tape servers.
- The `ctaeos` chart is not exactly very pretty and also not very flexible. This should be replaced by a more generic disk buffer chart.
- All the pods write their logs to the same mount (with no way to turn this off), making it unsuitable for a production usecase.
- The GRPC frontend configuration has not been tested/implemented yet.
- There is no systemd support (do we even want this in a containerized setup?).
- It is not yet possible to redeploy individual subcharts of the `cta` chart.
- It only supports handling a single library. What if there are multiple libraries? How to handle multiple library configurations?
- Authentication is currently somewhat hardcoded.

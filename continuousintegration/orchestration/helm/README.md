# CTA Runner helm chart

This is the helm chart to easily automate the setting up the testing environnment.

## Contents

1. [Helm installation](#helm-installation)
2. [Helm chart organisation](#helm-chart-organisation)
3. [CTA](#cta)
4. [Init](#init-pod)
5. [Usefull commands for developers](#usefull-commands-for-developers)
6. [How to improve in the future](#how-to-improve-in-the-future)

## Helm Installation

You need to apply following commands:

```bash
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
mkdir ~/bin
USE_SUDO=false HELM_INSTALL_DIR=${HOME}/bin ./get_helm.sh
```

CTA Runner is using two helmcharts: `cta` which contains all of the project pods, including `EOS` pod and `init` chart, which sets up PVCs, keypass'es and postgresql database.

## Helm chart organisation

CTA CI is organised into two main charts:
- **CTA** chart, which is organised into smaller charts containing each of the project pods.
- **Init** chart, which job is to prepare the environment (Setup the postgresql database, prepare keytabs for the CTA to use)



## CTA

CTA chart contains 5 smaller charts on its own:
- client
- ctafrontend
- ctaeos
- ctacli
- tapes

### Values.yaml structure
Each of the charts beside `tapes` chart follow the following values.yaml structure:

| Key | Type  | Description |
|-----|------|-------------|
| customEntrypoint | object | By using `command` and `args` subfields you can define the custom entrypoints for the pod to run on the image.
| image | string |It has higher priority than `global.image`|
| imagePullSecret | string| name of the secret containing docker registry |
| isPriviliged | bool  | Sets the pod to run in privilliged mode  |
| nameOverride | string  | Overwrites the name for the pods in the deployment if specified. Usefull when you wish to name the deployment in other way than you specified via `helm install` command |
| readinessProbe | object  | Allows to add a command to apply command to check for example if the `eos` is prepared |
| volumes | object  | Defines volumes for the pod |
| volumeMounts | object  | Defines mountings for the pod  |

In case of the tapes pod situation looks a bit different:


| Key | Type  | Description |
|-----|------|-------------|
| tapes.(tape).containers.command | string | Change the entrypoint for the docker image
|tapes.(tape).containers.args | string | arguments that are to be passed with command
| tapes.(tape).containers.image | string | image name, path where to find it. It is taken with higher priority than `global.image` |
| tapes.(tape).isPriviliged | bool  | Sets the pod to run in privilliged mode
| tapes.(tape).labels | object  | Defines labels for the pod |
| tapes.(tape).volumes | object  | Defines volumes for the pod |
| tapes.(tape).containers.volumeMounts | object  | Defines mountings for the pod  |
| tapes.(tape).containers.name | string  | name of the container |
| tapes.(tape).containers.isPrivilliged | bool  | Run container in privilliged mode|


### Global config
Each of the pods has the possibility to have set up additional values by `global`


| Key | Type | Description |
|-----|------|-------------|
|global.image | string  | sets image version that is being used by the CTA pods in the chart.
|global.imagePullSecrets| string  | Name that contains password to the image registry.
|global.volumeMounts | object | sets mounting for each of the pod in the chart. They are provided as additional ones.
|global.volumes | object | sets volume for each of the pod in the chart. They are provided ass additional ones



#### Configmaps

To define configmaps to use within the chart you must provide their definition next to `configs` key.

| Key | Type  | Description |
|-----|------|-------------|
| configs[0].name | string| Name for the configmap that will be used |
| configs[0].labels | object list |  Key value pairs indicating the type of the label and name of the label |
| configs[0].data | object list |   Data that configmap stores as a list of key - value pairs


### Examples
#### Configs
```yaml
configs:
- name: objectstore-config
    labels:
      config: objectstore
      type: file
    data:
      objectstore.file.path: /shared/%NAMESPACE/objectstore
      objectstore.type: file
```

##### Custom entrypoint:
```yaml
customEntrypoint:
  command: ["/bin/sh", "-c"]
  args:
  - |
    cat /tmp/eos.keytab > /etc/eos.keytab;
    chmod 600 /etc/eos.keytab;
    /opt/run/bin/client.sh;
```
Sets shell to apply multiple commands within pod


#### Volumes and VolumeMounts:

```yaml
volumeMounts:
- mountPath: /root/ctaadmin2.keytab
  name: ctaadmin2-keytab
  subPath: ctaadmin2.keytab
volume:
- name: ctaadmin2-keytab
  secret:
    secretName: ctaadmin2-keytab
```
Sets up a volume and mounts a file in a given pod within `values.yaml` file

#### Singular tpsrv configuration
```yaml
tpsrv01:
    labels:
      k8s-app: ctataped
    containers:
    - name: rmcd
      isPriviliged: true
      command: ['/opt/run/bin/rmcd.sh']
      args: ["none"]
      env:
      - name: MY_CONTAINER
        value: "rmcd"
      - name: driveslot
        value: "0"
      - name: TERM
        value: "xterm"
      volumeMounts:
      - mountPath: /shared
        name: shared
      - mountPath: /etc/config/library
        name: mylibrary
      - mountPath: /mnt/logs
        name: logstorage

    - name: taped
      isPriviliged: true
      command: ["/bin/sh", "-c"]
      args:
      - |
        cat /tmp/dump/cta/cta-tpsrv-tmp.sss.keytab > /etc/cta/cta-taped.sss.keytab;
        chmod 600 /etc/cta/cta-taped.sss.keytab;
        /opt/run/bin/taped.sh;
      env:
      - name: MY_CONTAINER
        value: "taped"
      - name: eoshost
        value: "mgm"
      - name: TERM
        value: "xterm"
      - name: driveslot
        value: "0"
      volumeMounts:
      - mountPath: /shared
        name: shared
      - mountPath: /etc/config/objectstore
        name: myobjectstore
      - mountPath: /etc/config/database
        name: mydatabase
      - mountPath: /etc/config/library
        name: mylibrary
      - mountPath: /etc/config/eos
        name: eosconfig
      - mountPath: /mnt/logs
        name: logstorage
      - mountPath: /tmp/dump/cta/cta-tpsrv-tmp.sss.keytab
        name: tape-sss
        subPath: cta-tpsrv-tmp.sss.keytab
    volumes:
    - name: shared
      hostPath:
        path: /opt/cta
    - name: myobjectstore
      configMap:
        name: objectstore-config
    - name: mydatabase
      configMap:
        name: database-config
    - name: mylibrary
      configMap:
        name: library-config
    - name: eosconfig
      configMap:
        name: eos-config-base
    - name: logstorage
      persistentVolumeClaim:
        claimName: claimlogs
    - name: tape-sss
      secret:
        secretName: tpsrv-sss-keytab
```
It creates a **tpsrv01** pod with:
- two containers named `ctataped` and `rmcd`
- both of them contain configuration of environment variables as a list
- both contain mouning of the configuration files and secrets


## Init

### Global values

| Key | Type | Description |
|-----|------|-------------|
|image | string  | sets image version that is being used by the kdc and init pods in the chart.
|imagePullSecrets| string  | Name that contains password to the image registry.

### Init pod

| Key | Type | Description |
|-----|------|-------------|
|volumes| object  | Declares volumes which the kdc pod will be using.
|volumeMounts| object  | Mounts volumes declared for the pod
|customEntrypoint.command| list  | Command to be invoked on the image.
|customEntrypoint.args| list  | Arguments that are to be passed with the command
|env | list |  List of environment variables to the pod|
|image| string   | Docker image from which init will be built. It has priority over `.Values.global.image` |
|nameOverride| string | Name to overwrite pod deployment name.
|isPriviliged| bool | Launches image with privilliges

### PVC config

To create storage volume claim, you need to add following keys under `pvcs` key

| Key | Type | Description |
|-----|------|-------------|
| pvcs.<name_of_your_claim>.accessModes | string |   Access mode that you would like storage volume to have. Possible values: `ReadWriteOnce` `ReadWriteMany` `ReadOnlyMany` `ReadWriteOncePod`. More details [here](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes)  |
| pvcs.<name_of_your_claim>.annotations | string   |  |
| pvcs.<name_of_your_claim>.selectors | list   | List of selectors on which claim will pick storage
| pvcs.<name_of_your_claim>.storage | string   | Space that the claim will reserve. |



### KDC config

KDC generates the keytabs as secrets on k8s cluster that are later used with tests.
To be able to do that, it needs to have service account that has rights to create secrets.

| Key | Type  | Description |
|-----|------|-------------|
|kdc.service.ports | object  | Ports that are supposed to be shared for communication purposes. |
|kdc.service.ports.name | str  | name of the port to be refered later to |
|kdc.service.ports.port | int |  Port number to be shared. |
|kdc.service.ports.protocol | string | Protocol to be used for that port. |
|kdc.pod.volumes| object  | Declares volumes which the kdc pod will be using.
|kdc.pod.volumeMounts| object  | Mounts volumes declared for the pod
|kdc.pod.args| list |    Arguments to be provided for the command
|kdc.pod.command| list | Command to be invoked on the image.
|kdc.pod.env | list |   List of environment variables to the pod|
|kdc.pod.image| string  |  Docker image from which kdc will be built |
| kdc.serviceAccount.name |string | Secret account name to bind with the pod
| kdc.serviceAccount.rules | object| Rules that the service account has access to
### Examples of values:
#### service account
```yaml
serviceAccount:
  name: secret-creator
  rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get", "list", "watch", "create"]
```
creates a service account with name `secret-createor` which will bind it with kdc pod with that name. The service account listed has following `rules`:
- `apiGroups`: indicates that we are using Kubernetes API
- `resources`: grants access to the listed resources (here only for `secrets`)
- `verbs`: words that allow the account for the following actions: list, retrieve create, and watch secrets.

#### service
```yaml
service:
  ports:
  - name: kdc-tcp
    port: 88
    protocol: TCP
  - name: kdc-udp
    port: 88
    protocol: UDP
```
creates a headless service that opens `UDP` and `TCP` traffic on port number 88

#### PVCS

```yaml
pvcs:
  claimstg:
    selectors:
      type: stg
    storage: 3Gi
    accessModes:
      - ReadWriteMany
```
Defines a PVC:
- that will have name `claimstg`
- has defined selector of type `stg`
- claims `3Gi` of storage
- has access mode `ReadWriteMany`

## Basic workflow

### Installing the chart
The helm charts contain already prefilled values.yaml files that will launch working CTA instance if launched in order:
1. Init
2. CTA

To install a helm chart you will need to invoke the following command:

```bash
  helm install <name of the release> <directory where the chart exists>
```
For example command `helm install cta ./cta/` invoked in `helm_instances` directory will install `cta` chart with all of its subcharts (`ctacli`, `client`, etc).

Command arguments that might be handy in work with charts:

- `-n <namespace_name>` - namespace on which chart will be installed. The namespace must exists.
- `-f <filename>` - allows to override the default values.yaml file with yours

More can be found on the [documentation](https://helm.sh/docs/helm/helm_install/) site

### Modyfing the values.yaml files for subcharts

Here will be focused the recommended way to do it. If you would like for example change parts of the configuration for the ```ctafrontend``` pod, you would need to do it like given below

```yaml
# Its the base for the CTA chart
global:
  imagePullSecret: "ctaregsecret"
  image: gitlab-registry.cern.ch/cta/ctageneric:7894245git8a4cc01b
  volumes:
  - name: versionlock-list
    configMap:
      name: versionlock-conf
  volumeMounts:
  - mountPath: /etc/dnf/plugins/versionlock.list
    name: versionlock-list
    subPath: versionlock.list

# Here begins section for subchart
ctafrontend:
  image:  my_custom_image
  env:
  - name: "foo"
    value: "bar"
# Other configurations that you would like to change....
...
values.yaml
```
Whenever you wish to modify the values for the subchart, you need to provide corresponding name of that subchart, and under its key you can provide the values and other keys that the subchart supports. For example file above defines the values for the `ctafrontend` subchart which overrides the following fields
- `image` adds the container image that will be used instead of `global.image` (by default `image` is null)
-  `env`: provides the list of the environment variables that replaces the default ones in `ctafrontend/values.yaml`

If you launch it will `helm install` command described before the `ctafrontend` pod will have `image` and `env` fields modified and the rest of the fields will remain unchanged


### Usefull commands for developers

If you wanted to restart one of the pods and provide another image for example in ctafrontend pod, you may do this:

1. Modify values.yaml file for the subchart as described above.
2. Use helm upgrade command (must be withing main chart directory)
```bash
helm upgrade -f values.yml cta ./ --version 0.1.0
```
It will upgrade the chart and restart the ctafrontend pod to allow for changes to apply.

## How to improve in the future

There are some ways to improve this chart, but it hasn't done due to the time issues, but it will be listed here.

- Replace the ctaeos subchart with the EOS chart.
- Use secret vault for the secrets, so in the future the chart could be utilised in the production.
- Create setup using deployments, so it could be used in the production
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

CTA chart contains 4 smaller charts on its own:
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


#### Examples
- volumes and volumeMounts:

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

- customEntrypoint:
```yaml
customEntrypoint:
  command: ["/bin/sh", "-c"]
  args:
  - |
    cat /tmp/eos.keytab > /etc/eos.keytab;
    chmod 600 /etc/eos.keytab;
    /opt/run/bin/client.sh;
```
Sets shell to apply multiple commands within pod (in this case in client pod)

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


Examples:
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


## Usefull commands for developers

If you wanted to restart one of the pods and provide another image for example in ctafrontend pod, you may do this:

1. Modify values.yaml file by providing:
```yaml
ctafrontend:
  image: <your_docker_image>
```
2. Use helm upgrade command (must be withing main chart directory)
```bash
helm upgrade -f values.yml cta ./ --version 0.1.0
```
I will upgrade the chart and restart the ctafrontend pod to allow for changes to apply.

## How to improve in the future

There are some ways to improve this chart, but it hasn't done due to the time issues, but it will be listed here.

- Replace the ctaeos subchart with the EOS chart.
- Use secret vault for the secrets, so in the future the chart could be utilised in the production.
- Create setup using deployments, so it could be used in the production
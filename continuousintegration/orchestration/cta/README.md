# CTA Runner helm chart

This is the helm chart to easily automate the setting up the testing environnment.

## Helm Installation

You need to apply following commands:

```
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
```

Be sure that you run it as an account with enough privilliges (In case of working on CTA VM you should do it on a root account)

## Installing CTA

It's simple. You need only to invoke following commands

```
helm dependency update # It downloads required dependencies
helm install <name-of-the-releases> -n <your_namespace> -f <your_values.yaml> <chart_directory>
```


## Requirements

Thos are downloaded via `helm dependency update` command.

| Repository | Name | Version |
|------------|------|---------|
| oci://registry.cern.ch/eos/charts | eos(server) | 0.2.0 |


## Values

Here are stored documented values.yaml contents.


### Configmaps

To define configmaps to use within the chart you must provide their definition next to `configs` key.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| configs[0].name | string| - | Name for the configmap that will be used |
| configs[0].labels | object list | - | Key value pairs indicating the type of the label and name of the label |
| configs[0].data | object list | - |  Data that configmap stores as a list of key - value pairs
| configs[0].filename | string | - | Name of the file from which should be configmap created. It is created as a file as well. Files from which configmaps will be created must be stored under `configmaps` directory |


Examples:

- raw values
```
configs:
- name: objectstore-config
    labels:
      config: objectstore
      type: file
    data:
      objectstore.file.path: /shared/%NAMESPACE/objectstore
      objectstore.type: file
```

- file content
```
configs:
- name: keypass-names-config
    file: keypass-names.txt
    labels:
      config: keypass-names
```
### PVC config

To create storage volume claim, you need to add following keys under `pvcs` key

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| pvcs.<name_of_your_claim>.accessModes | string | |   |
| pvcs.<name_of_your_claim>.annotations | string |  |  |
| pvcs.<name_of_your_claim>.selectors | list |  | List of selectors on which claim will pick storage |
| pvcs.<name_of_your_claim>.storage | string |  | Space that the claim will reserve.  |

### KDC config

KDC generates the keytabs as secrets on k8s cluster that are later used with tests.
To be able to do that, it needs to have service account that has rights to create secrets.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
|kdc.service.ports | object | - | Ports that are supposed to be shared for communication purposes. |
|kdc.service.ports.name | str | - | name of the port to be refered later to |
|kdc.service.ports.port | int| - |  Port number to be shared. |
|kdc.service.ports.protocol | | | Protocol to be used for that port. |
|kdc.pod.volumes| object | null | Declares volumes which the kdc pod will be using.
|kdc.pod.volumeMounts| object | null | Mounts volumes declared for the pod
|kdc.pod.args| list | |    Arguments to be provided for the command
|kdc.pod.command| list | | Command to be invoked on the image.
|kdc.pod.env | list |  | List of environment variables to the pod|
|kdc.pod.image| string  |  | Docker image from which kdc will be built |


### EOS config

For the more accurate docs fo each of the subchart that EOS uses please refer to its [repo](https://gitlab.cern.ch/eos/eos-charts/-/tree/master?ref_type=heads). Here are provided only required values to allow for CTA to work with EOS.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| eos.global.repository | string | `"gitlab-registry.cern.ch/dss/eos/eos-all"` |  |
| eos.global.tag | string | `"5.0.31"` |  |
| eos.global.sssKeytab.secret | string | `"eos-sss-keytab"` | Name of the secret containing eos-sss-keytab to use.|
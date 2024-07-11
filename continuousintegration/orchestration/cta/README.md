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

Its simple. You need only to invoke following command

```
helm install <name-of-the-deployment> -f <your_values.yaml> <chart_directory>
```
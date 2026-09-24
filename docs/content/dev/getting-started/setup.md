# Setting up a Development Environment

This document describes how to get a CTA+EOS instance, with CTA built from source, running in a standalone VM (Virtual Machine) with Alma9 as operating system.

## Initial setup

Before you start your development. Git should be configured to use your CERN email and username using the following commands:

```bash
git config --global user.email "<YOUR CERN EMAIL>"
git config --global user.name "<YOUR CERN USERNAME>"
```

!!! tip

    Optionally (but recommended), configure which text editor git should be using for any text operations:

    ```sh
    git config --global core.editor "vim"
    ```

## Pre-commit Hooks

Pre-commit hooks are a convenience tool for developers to get feedback earlier. The CTA repository provides a number of pre-commit hooks:

- secret detection
- C++ formatting
- Pythong formatting and linting

To use pre-commit hooks, you must have installed the [pre-commit tool](https://pre-commit.com/):

```sh
pip install pre-commit
```

Then the pre-commit hooks in the repository can be enabled by navigating to the root of the repository and executing:

```sh
pre-commit install
```

The first time the hooks run might take a bit longer as the hooks will need to be installed.

Before every commit, the hooks specified in `.pre-commit-config.yaml` will be automatically triggered.
Depending on the hook, it might simply fail a check (e.g. secret detection), or it might format files for you automatically (e.g. `clang-format`).

When hooks format things automatically, you have to explicitly stage the changes (`git add`) and commit again. After a hook failure, you can check with `git diff` to see if the hook made any changes.

!!! tip

    If you want to temporarily deactivate all pre-commit checks run `git commit --no-verify` or `SKIP=<hook_id>,... git commit` to specify a list of hooks to skip.
    While this is not recommended, it is still safe to do so because all pre-commit checks are also performed in CI. It is simply less convenient to fix it afterwards.

### Detect-secrets hook

This hook detects if any secrets would be added by referring to the `.secrets.baseline` file.

False positives may cause this pre-commit hook to fail. The best way to deal with such and error is the following:

1. Run the `detect-secrets` tool through the hook:

    ```sh
    pre-commit run detect-secrets scan --baseline .secrets.baseline
    ```

2. Add `.secret-baseline` file to the commit.

For more information you can check the official [documentation](https://github.com/Yelp/detect-secrets/blob/master/README.md)

## Installing the Kubernetes Environment

Follow the link below to set up a virtual environment to run CTA in.

<div class="grid cards" markdown>

-   :simple-kubernetes:{ .lg .middle } [__CTA CI Node Setup__](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup)

    ---

    Find instructions on how to set up a CI Kubernetes environment for CTA.

    [:octicons-arrow-right-24: https://gitlab.cern.ch/cta/ci/cta-ci-node-setup](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup)

</div>

!!! tip

    For building your own RPMs/Docker images, you don't need this Kubernetes environment. `cta-dev` supports Podman & Docker up until the `deploy` stage.

### Testing Kubernetes

Kubernetes is automatically started at boot time for user `cirunner`. While logged in as the `cirunner`, you should now be able to run `kubectl` commands.

!!! example

    Running `kubectl get namespaces` should output something along the lines of:

    ```text
    default           Active   2m16s
    kube-node-lease   Active   2m16s
    kube-public       Active   2m16s
    kube-system       Active   2m16s
    ```


### Using ssh Keys on the VM

During development it is convenient to be able to use ssh keys from the host machine on the VM. For example to execute git related actions.
To do this, start by adding the ssh key you want to use on the VM to the ssh agent:

```sh
ssh-add <your ssh key> # e.g. ssh-add ~/.ssh/id_rsa
```

After doing this, you can ssh into the VM with the `-A` flag enabled and you should be able to use this key:

```sh
ssh -A cirunner@<yourvm>
```

## Containerised Compilation and Deployment

To start, ssh into the machine as `cirunner` and navigate to a directory of your choice to clone the CTA repo. In this example, we will be using `/home/cirunner/`:

```sh
ssh -A cirunner@<yourvm>
git clone ssh://git@gitlab.cern.ch:7999/cta/CTA.git
cd CTA
git submodule update --init --recursive
# Install pre-commit recommended
```

You should now have a fully initialized repository on the VM.

To compile and deploy CTA on the local Kubernetes cluster, execute the following script:

```sh
cd ci/
./cta-dev.sh install # This will allow you to use `cta-dev` from anywhere and set up the required environment for the system tests
cta-dev up
```

This will take quite a few minutes (especially the first time), but after this script has finished, you should see a number of pods running in the `dev` namespace:

```sh
NAME                                READY   STATUS      RESTARTS   AGE
auth-configure-keycloak-job-59fms   0/1     Completed   0          66s
auth-generate-secrets-job-8whln     0/1     Completed   0          84s
auth-kdc                            1/1     Running     0          84s
auth-keycloak                       1/1     Running     0          84s
cta-catalogue-postgres-db           1/1     Running     0          84s
cta-catalogue-reset-job-b5c4w       0/1     Completed   0          76s
cta-cli-0                           1/1     Running     0          47s
cta-client-0                        1/1     Running     0          47s
cta-frontend-0                      1/1     Running     0          47s
cta-maintd-d6767b99-2grbr           1/1     Running     0          47s
cta-maintd-d6767b99-lr6mm           1/1     Running     0          47s
cta-scheduler-reset-job-c5d45       0/1     Completed   0          84s
cta-tpsrv01-0                       2/2     Running     0          47s
cta-tpsrv02-0                       2/2     Running     0          47s
eos-fst-0                           1/1     Running     0          47s
eos-mgm-0                           1/1     Running     0          47s
eos-qdb-0                           1/1     Running     0          47s
```

That's it; you have a working dev environment now. `cta-dev` is the main tool for local development. For more information, run `cta-dev --help` and have a look at [the cta-dev page](./cta-dev.md).

## Simple workflow

The above will spawn a very barebones setup with an empty Catalogue and no proper EOS configuration. This is of course not very interesting to play around with, so let’s populate our system with some data. To do this, we only want to run the setup part of the system tests:

```
cta-dev test setup
```

Now Let's familiarise ourselves with the different pods and the archival workflow.
The `cta-cli-0` pod can be used to execute `cta-admin` commands. Start by opening a shell in this pod:

```shell
kubectl exec -it -n dev cta-cli-0 -- bash
```

To see the version that we are running on:

```shell
cta-admin version
```

At this point, **you should have a look at the [Introductory Walkthrough](./walkthrough.md)**, which will make you familiar with the main workflows in CTA, as well as the most important commands in `cta-admin`.


### More tests

If you want to do a more elaborate test, you can run one of the system tests such as `ci/system_tests/tests/client_test.py`. Luckily, `cta-dev` can run these tests for you without having to manually invoke `pytest`:

```sh
cta-dev deploy # Just to ensure we start fresh. Alternatively, add the --teardown-first flag to the test command
cta-dev test client
```

Note that this may take about 10 minutes to complete.
___

## Cheat Sheet: A quick summary of how to work with pods

To see all the pods in a namespace (e.g. `dev`), you can use:

```shell
kubectl get pods -n dev
```

In general, to open a shell on any desired `<pod>`, you can execute the following:

```shell
kubectl exec -it -n dev <pod> -- bash
```

Some pods have multiple containers in them. To specify which container to open the shell in, add the `-c` flag:

```shell
kubectl exec -it -n dev <pod> -c <container> -- bash
```

To execute a command directly without opening a dedicated shell (e.g running `cta-admin version`), you can do the following:

```shell
kubectl exec -it -n dev cta-cli-0 -- cta-admin version
```

Most pods will log directly to `stdout/stderr`. To see this, use the `kubectl logs` command:

```shell
kubectl logs <pod> [-c <container>] -n dev
```

Some of the process produce multiple log files (e.g. the XRootD Frontend or the EOS pods). You can find all of these logs in `/var/log` in the corresponding container.

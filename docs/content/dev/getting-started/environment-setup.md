# Environment Setup

This walkthrough prepares a full CTA development instance on an AlmaLinux 9 VM, using Kubernetes, virtual tape hardware, and an EOS disk buffer. If you only need to edit files or build packages, see [Choose where to work](prerequisites.md#choose-where-to-work). Repository access, cloning, Git configuration, and hooks are covered in [Prerequisites & Access](prerequisites.md).

## Prepare the development machine

Follow the [CTA CI Node Setup instructions](https://gitlab.cern.ch/cta/ci/cta-ci-node-setup) to prepare the AlmaLinux 9 VM, including the `cirunner` account, container tools, Kubernetes, and virtual tape hardware. The `cta-dev install` step below checks that the machine has the prerequisites needed for development.

### Connect to the VM

From your workstation, connect to the development account:

```sh
ssh cirunner@<yourvm>
```

If you use SSH for Git access and keep the repository key on your workstation, you can instead load it into your local SSH agent and forward the agent when connecting:

```sh
ssh-add <path-to-your-ssh-key>
ssh -A cirunner@<yourvm>
```

Agent forwarding is optional; it is not needed for the GitHub HTTPS clone route. See [Repository authentication](prerequisites.md#repository-authentication) for Git access setup.

## Prepare your checkout

On the development machine, follow [Clone the repository](prerequisites.md#clone-the-repository), [Configure Git](prerequisites.md#configure-git), and [Enable pre-commit](prerequisites.md#enable-pre-commit). If the checkout is already prepared on this machine, use it directly.

Run the remaining commands from the repository root on the development machine.

## Install cta-dev

```sh
./ci/cta-dev.sh install
```

The installer checks the development prerequisites and sets up the `cta-dev` command and the system-test Python environment. If it reports missing tools or configuration, resolve the reported issues and rerun it before continuing.

After installation succeeds, restart your shell, ensure `~/.local/bin` is on `PATH`, and check the command is available:

```sh
cta-dev --help
```

See [Installation and worktree selection](../reference/tools-and-environment/cta-dev.md#installation-and-worktree-selection) for installer details.

## Build and deploy

Build CTA packages and images, then deploy the development instance in the `dev` namespace:

```sh
cta-dev up --namespace dev
```

This walkthrough uses the default EOS deployment. For dCache or other deployment options, see [cta-dev deployment variants](../reference/tools-and-environment/cta-dev.md#deployment-options).

!!! note "Redeploying an instance"

    Running `cta-dev up` again replaces the existing managed development deployment in the selected namespace and resets its catalogue and scheduler. Use it when you intend to rebuild and recreate the instance.

Inspect the deployed pods:

```sh
kubectl get pods -n dev
```

Service pods should become ready, with all their containers counted in the `READY` column. Setup jobs can show `Completed`. For pods that remain pending or fail to start, use the commands in [Working with Development Pods](../reference/tools-and-environment/development-pods.md) to inspect their logs.

## Initialize and verify the deployment

Prepare the system-test fixtures, including catalogue resources, virtual tape setup, and EOS configuration:

```sh
cta-dev test --namespace dev setup
```

Check that the administrative client can communicate with CTA:

```sh
kubectl exec -it -n dev cta-cli-0 -- cta-admin version
```

The command should return version information without an error. This verifies administrative communication with the initialized instance.

## Next: archive and retrieve a file

Before making your first code change, follow the [EOS archive and retrieve walkthrough](archive-retrieve-walkthrough.md) using the instance you just prepared. It introduces the basic CTA workflows and administrative commands by taking you through archiving a file to tape and retrieving it to disk.

Working through these steps gives you a practical foundation for understanding the code and tests. Afterwards, continue with [Your First Change](first-change.md). For further validation of your environment, see [Running system tests](../reference/testing/system-tests.md).

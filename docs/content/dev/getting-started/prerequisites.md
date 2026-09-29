# Prerequisites & Access

Prepare access to the CTA repository and a checkout ready for development. Build tools, containers, Kubernetes, and running CTA are covered in [Environment Setup](environment-setup.md).

## Repository access

CTA source code and issues are hosted in the [CTA GitLab project](https://gitlab.cern.ch/cta/CTA/). The source is also available through the [GitHub mirror](https://github.com/cern-cta/CTA).

CTA is open source, and contributions are welcome. If you would like to contribute, post on the [community forum](https://cta-community.web.cern.ch/) or email [cta-support@cern.ch](mailto:cta-support@cern.ch) to discuss your ideas and the contribution route that fits your situation.

- **CERN GitLab:** Direct developer access requires a CERN computing account and project permissions. Account eligibility is subject to CERN’s access requirements; contacting the team does not guarantee an account or developer access.
- **GitHub:** Contributors without CERN developer access can work in a fork of the GitHub mirror and submit a pull request. A CTA maintainer brings the contribution into GitLab for CI and merging. This manual handoff is subject to maintainer availability; please agree on the scope with the team first. See [Contributing through GitHub](../contributing/github.md).

## Repository authentication

For direct CERN GitLab development, use SSH to clone and push changes. Follow the [CERN GitLab SSH instructions](https://gitlab.cern.ch/help/user/ssh.md) to configure your key and test the connection. Make the key available on the machine from which you access the repository; for a development VM, see [Connect to the VM](environment-setup.md#connect-to-the-vm).

For the GitHub route, use your GitHub account to fork the mirror and authenticate pushes to your fork. This route does not require a CERN computing account.

## Choose where to work

Choose the environment based on what you need to do:

| Task | Where to work |
| --- | --- |
| Edit documentation, inspect the source, or prepare changes without building CTA | Use a local checkout on your workstation. A development VM is not needed. For documentation previews, follow [Documentation Changes](../contributing/documentation.md). |
| Build CTA RPMs or container images without running a deployment | Use a machine with a working Podman installation and the [build requirements](../guides/tools-and-environment/building-images-and-packages.md#requirements). Kubernetes is not needed for these build stages. If you do not have a suitable machine, use the development VM. |
| Run CTA, exercise archive/retrieve workflows, or run system tests | Follow [Environment Setup](environment-setup.md) to prepare the AlmaLinux 9 development VM with Kubernetes and virtual tape hardware. If you already have a machine prepared with the CTA development-node setup, use that instead. |

For a first code change that you want to build and test against a running CTA instance, use the development VM. Prepare it before cloning, then return here from that machine. You do not need a separate checkout on your workstation first.

Configure Git and install pre-commit hooks where you will make commits. In the VM-based workflow described here, that is the VM. If you edit and commit on your workstation instead, configure them there; the build and deployment commands still run on the prepared development machine.

The checkout and hook instructions below require Git, Python 3, and Python virtual-environment support. Set up hooks in each checkout where you make commits.

## Clone the repository

Choose the clone command for your contribution route. In your chosen working directory:

### CERN GitLab

```sh
git clone ssh://git@gitlab.cern.ch:7999/cta/CTA.git
cd CTA
git submodule update --init --recursive
```

### GitHub fork

First fork [cern-cta/CTA](https://github.com/cern-cta/CTA) into your GitHub account. Then clone your fork:

```sh
git clone https://github.com/<YOUR GITHUB USERNAME>/CTA.git
cd CTA
git remote add upstream https://github.com/cern-cta/CTA.git
git submodule update --init --recursive
```

Configure GitHub authentication before pushing changes to your fork. See [Contributing through GitHub](../contributing/github.md) for the submission and review workflow.

For either route, run the remaining commands from the repository root.

## Configure Git

Set the name and email address to record on your commits. Use your author name, rather than assuming it must be your account username:

```sh
git config user.name "<YOUR NAME>"
git config user.email "<YOUR EMAIL>"
```

These settings apply only to this checkout. Use `git config --global` instead if the same identity should apply to all repositories for your user on this machine.

Optionally configure your preferred editor, for example:

```sh
git config core.editor "vim"
```

## Enable pre-commit

Install `pre-commit` in a dedicated Python environment on the machine where you commit changes:

```sh
python3 -m venv ~/.local/share/cta-pre-commit
~/.local/share/cta-pre-commit/bin/python -m pip install pre-commit
~/.local/share/cta-pre-commit/bin/pre-commit install
```

Keep this Python environment available: the installed Git hook uses it when you commit. If you already have a working `pre-commit` installation, run `pre-commit install` from the repository root instead.

The repository's `.pre-commit-config.yaml` defines the checks. The first run may take longer while hook environments are installed. Some checks report failures; others automatically format files. After a hook changes files, inspect the diff, stage the intended changes again, and retry the commit.

For hook troubleshooting, see [Pre-commit Hooks](../guides/tools-and-environment/pre-commit.md).

## Continue with environment setup

Continue with [Environment Setup](environment-setup.md). If you have already prepared the development machine, proceed to [Install cta-dev](environment-setup.md#install-cta-dev).

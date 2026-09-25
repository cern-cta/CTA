# Getting Started

This section provides comprehensive guides, references, and best practices to help with the development of CTA. Before making any contributions to CTA, please carefully read through the contributing instructions linked below.

## How to Contribute

For our version control management practice we use trunk-based development. This means we have a single `main` branch where we frequently merge small features, bug fixes and maintenance updates. It is important that changes are kept small and focussed.
The branch `main` is protected. It is therefore not possible to directly push new commits. The only way to add changes is with a _Merge Request_ (MR).

1. Request access to work on the CTA repository (contact us)
2. [Set up a development environment](./getting-started/setup.md)
3. [Choose or create an issue](./contributing/issues.md) to work on
4. [Create a branch](./contributing/branches.md) on the CTA repository
5. Make your changes on this branch (see [Coding Conventions](../conventions/coding/general.md))
6. [Open a merge request](./contributing/merge-requests.md) from your branch into `main`
7. Ensure all necessary checks pass and assign/request a reviewer
8. Wait for your work to be reviewed and eventually merged

## CTA Technologies

CTA is written primarily in C++. It uses CMake or the build system in combination with a [SPEC file](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/packaging_and_distributing_software/packaging-software_packaging-and-distributing-software#assembly_what-a-spec-file-is_packaging-software) to generate the binaries/RPMs.

For testing/development purposes, a combination of Kubernetes and Helm is used for spawning CTA in containers. Many of the Continuous Integration (CI) scripts are written in Bash or Python.

- To learn more about the Continuous Integration setup used for CTA, see the `Continuous Integration` section.
- To read more about architecture of CTA, see the `Architecture` section.

## Licensing

CTA is licensed under [GPL Version 3](https://gitlab.cern.ch/cta/CTA/-/blob/main/COPYING?ref_type=heads) and uses [SPDX](https://spdx.dev) identifiers for machine-readable licensing information.
All files must therefore include an SPDX header at the top of the file:

```text
SPDX-FileCopyrightText: <year of creation> CERN
SPDX-License-Identifier: GPL-3.0-or-later
```

External contributors are encouraged to add their own `SPDX-FileCopyrightText` line for new files or significant contributions. Files may therefore contain multiple copyright lines.

All files must include an SPDX license identifier indicating GPL-3.0-or-later. License compliance is checked using REUSE.

## Useful Links

- [CTA Repository](https://gitlab.cern.ch/cta/CTA/)
- [Issues](https://gitlab.cern.ch/cta/CTA/-/issues)
- [Development Overview](https://gitlab.cern.ch/cta/CTA/-/boards/32164)
- [Priority Management](https://gitlab.cern.ch/cta/CTA/-/boards/26993)

---
title: Development overview
---

# Development

This section is for people changing CTA, its tooling and documentation, or developing disk-system integrations. It covers source code, interfaces, tests, and contribution procedures. To install or run an existing deployment, use [Operations](../ops/index.md).

## CTA Technologies

CTA's core services are written primarily in C++. The wider CTA tooling also uses Python and Rust, with some tools maintained in separate repositories. Development and Continuous Integration (CI) scripts use Bash and Python.

CMake configures the C++ build, and RPM spec files define how the software is packaged. Development builds use containers; Kubernetes and Helm deploy CTA and its dependencies for development and system testing. See [Building Images & Packages](guides/tools-and-environment/building-images-and-packages.md) and [Testing CTA](guides/testing/index.md) for details.

## Find Your Way

The navigation has four groups:

- **Getting Started** introduces the repository and the path to a first change. Begin with [Prerequisites & Access](getting-started/prerequisites.md).
- **Contributing** covers proposing, submitting, and reviewing changes. Start with the [Contributing overview](contributing/index.md); release and infrastructure procedures are under [For Maintainers](contributing/maintainers/index.md).
- **[Development Guides](guides/index.md)** covers [development tools and environment](guides/tools-and-environment/cta-dev.md), [testing and CI](guides/testing/index.md), [conventions](guides/conventions/index.md), [disk-buffer integrations](guides/integrations/index.md), and [instrumentation](guides/instrumentation/index.md). Consult these pages as needed for your task.
- **[Implementation Internals](internals/index.md)** explains interface contracts, workflow implementation, component design, and shared libraries.

## Recommended Reading Order

1. Read the [Concepts introduction](../concepts/index.md), [component overview](../concepts/components/index.md), and [file workflows](../concepts/data-management/index.md) for background.
2. Check [Prerequisites & Access](getting-started/prerequisites.md).
3. Explore the [Project Structure](getting-started/project-structure.md).
4. Complete [Environment Setup](getting-started/environment-setup.md).
5. Work through the [EOS archive and retrieve walkthrough](getting-started/archive-retrieve-walkthrough.md) to learn the basic workflows using your development instance.
6. Follow [Your First Change](getting-started/first-change.md), consulting the relevant [Coding Conventions](guides/conventions/coding/general.md).
7. Use [Testing CTA](guides/testing/index.md) to choose and run tests.
8. Follow the [contribution guide](contributing/index.md) for your GitLab or GitHub route.

## How to Contribute

Keep changes small and focused. The [Contributing overview](contributing/index.md) explains how to discuss your proposal and choose between CERN GitLab and GitHub. See [Copyright conventions](guides/conventions/coding/copyright.md) for copyright and license metadata requirements.

## Useful Links

- [CTA Repository](https://gitlab.cern.ch/cta/CTA/)
- [Issues](https://gitlab.cern.ch/cta/CTA/-/issues)
- [Development Overview](https://gitlab.cern.ch/cta/CTA/-/boards/32164)
- [Priority Management](https://gitlab.cern.ch/cta/CTA/-/boards/26993)

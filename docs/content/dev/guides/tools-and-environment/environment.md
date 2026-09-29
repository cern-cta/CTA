# Development & Test Environment

## Purpose and scope

The CTA development environment provides a disposable instance for local development and automated tests. It runs CTA and its dependencies in Kubernetes, using Helm charts and configuration presets from `ci/orchestration/`. It supplies test infrastructure, including authentication services and virtual tape hardware, rather than a production deployment.

For initial machine preparation and your first instance, follow [Environment Setup](../../getting-started/environment-setup.md). For ongoing command usage, see [cta-dev Reference](cta-dev.md).

## What an instance contains

The exact services depend on the selected configuration.

| Component | Role in the environment |
| --- | --- |
| CTA services | Frontends, tape daemons, media-changer and maintenance services carry out CTA operations. Service images contain the locally built or selected published version. |
| Catalogue | Stores CTA metadata. Development configurations can provide a local PostgreSQL database or connect to an external database, such as the CERN development Oracle service. |
| Scheduler | Stores pending work and coordinates its execution, using the selected scheduler backend. |
| Disk system | EOS or dCache provides the disk-side integration used for archive and retrieve workflows. |
| Authentication | Test authentication services and credentials allow the components and clients to communicate. |
| Administrative and test clients | Provide tools for administering CTA and exercising workflows against the deployed instance. |

For the roles of CTA components beyond the test environment, see [Components](../../../concepts/components/index.md).

## Host requirements and isolation

The nodes running tape services need [mhVTL](virtual-tape-library.md) installed on the host, with virtual tape and changer devices accessible to the pods. Helm deployment alone cannot provide those devices, so an arbitrary Kubernetes cluster is not sufficient.

A namespace separates Kubernetes resources, but it does not isolate shared tape hardware or external databases. Concurrent instances need separate drives and media, coordinated access to changers, and separate database schemas or accounts where the tests modify shared state. Selecting a different namespace alone does not make concurrent use safe.

For CERN development Oracle access, see the [internal database instructions](https://tapeoperations.docs.cern.ch/dev/centrally_managed_resources/#oracle-dbs-for-development-and-ci). For shared runner administration, follow [CI Maintenance](../../contributing/maintainers/ci-maintenance.md).

## Configuration and lifecycle

Use [cta-dev deployment options](cta-dev.md#deployment-options) to select images, backends, and custom Helm values.

Deployment starts the services; test setup creates the resources needed to use them. The general strategy is to clean or reset resources at the start of deployment and testing, so each run begins from a known state rather than relying only on cleanup from the previous run. Follow the [archive and retrieve walkthrough](../../getting-started/archive-retrieve-walkthrough.md) for manual exploration or [System Tests](../testing/system-tests.md) for automated testing.

Redeployment resets the catalogue and scheduler, so save any diagnostic evidence first. Deleting a namespace does not necessarily clean up external databases or virtual tape media.

For inspection and troubleshooting, see [Working with Development Pods](development-pods.md) and [Debugging](debugging.md).

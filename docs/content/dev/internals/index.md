# Implementation Internals

Use this section to trace an operation through CTA code or understand the component you need to change. Start with [Concepts](../../concepts/index.md) for system responsibilities and terminology.

| Area | Start here |
| --- | --- |
| Interfaces & Protocols | [Shared interface contracts](interfaces.md) |
| Workflow Implementation | [Workflow overview](workflows/index.md) and the path for the operation you are changing |
| Components | [Workflow Frontend](components/workflow-frontend.md), [Admin Frontend](components/admin-frontend.md), [Catalogue](components/catalogue/index.md), [Scheduler](components/scheduler/index.md), [Tape Daemon](components/tape-daemon.md), [Maintenance Daemon](components/maintenance-daemon.md), and [Media Changer Daemon](components/media-changer-daemon.md) |
| Shared Libraries | [Service Runtime](service-runtime.md), [Telemetry Internals](telemetry.md), and [Readiness and Liveness](health-checks.md) |

For a first implementation tour, read the interface overview, follow an archive or retrieve request, then inspect the relevant component and backend. Detailed pages are reference material, not a required reading sequence. Explicit TODOs identify missing guidance; retained older descriptions are marked where they still need review.

Development and migration testing belong here. Release coordination belongs under [For Maintainers](../contributing/maintainers/index.md), and deployment or recovery procedures belong in [Operations](../../ops/index.md).

# Admin Frontend {#admin-api}

The **Admin Frontend** accepts operator requests from `cta-admin` and other administrative tools. It exposes CTA resource management and operational state separately from disk-system file workflows.

## Responsibilities

Operators use the Admin Frontend to inspect and manage tapes, drives, libraries, storage policies and queued work, and to submit operations such as repack.

## Relationships with other components

The Admin Frontend reads and updates the [Catalogue](catalogue.md) and its configured [Scheduler](scheduler.md) backend. The [Workflow Frontend](workflow-frontend.md) handles disk-system requests.

[Authentication](authentication.md) establishes the caller's identity. Administrative access also requires authorization through the catalogue's admin-user records. See [Authentication Configuration](../../ops/deploy-and-configure/configuration/authentication.md), [Admin Frontend Configuration](../../ops/deploy-and-configure/configuration/admin-frontend.md), and the [cta-admin reference](../../ops/tools/cta-admin.md).

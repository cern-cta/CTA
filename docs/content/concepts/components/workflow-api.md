# Workflow API

The Workflow API accepts archive, retrieve, and delete requests from the disk system and passes work to CTA's catalogue and scheduler. The disk system owns the client-facing namespace and interface; this API connects that system to CTA.

## Responsibilities and relationships

The API handles workflow requests over gRPC. The tape daemons transfer file data between the disk buffer and tape; that data does not pass through this API. See [File Lifecycle](../data-management/index.md), [Catalogue](catalogue.md), and [Scheduler](scheduler.md).

## Authentication and deployment

Workflow requests have their own [authentication boundary](authentication.md), separate from operator commands handled by the [Admin API](admin-api.md).

For settings and examples, see [Workflow API Configuration](../../ops/configuration/workflow-api.md).

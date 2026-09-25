# Admin API

The Admin API serves operator commands from `cta-admin` and other administrative tools. It provides access to CTA resource management and operational state independently of disk-system workflow requests.

## Responsibilities and relationships

Administrative commands inspect or change catalogue resources and scheduler state. The [Workflow API](workflow-api.md) handles archive, retrieve, and delete requests from the disk system separately.

## Authentication and deployment

Administrative access has its own [authentication boundary](authentication.md). Separating the APIs allows their access policies and deployment to be managed independently.

For settings and client configuration, see [Admin API Configuration](../../ops/configuration/admin-api.md).

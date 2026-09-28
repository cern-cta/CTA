!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Scheduling and Queues

Inspect and manage archive and retrieve work without needing scheduler implementation details.

## Inspect queues and mounts

TODO: Document queue age, pending work, mount activity, and common causes of delay.

## Failed and stuck requests

### Identify the failure

TODO: Document how to distinguish pending work, repeated transfer attempts, exhausted retries, and failed queues. Correlate archive/retrieve identifiers with logs and disk-system request state.

### Decide on intervention

TODO: Document retry, cancellation, and cleanup prerequisites, including whether the underlying failure has been resolved. Specify supported actions for each scheduler backend rather than importing historical retry counts or queue-edit commands.

### Verify the outcome

TODO: Document how to confirm that work resumed or cancellation completed, account for remaining copies, and check disk-system state. Use the [CTA administration reference](../../tools/cta-admin.md) for command syntax.

## Disk-system boundary

TODO: Document how to distinguish CTA queue failures from disk-system request tracking and transfer failures. System-specific checks belong in [Disk Buffer Integration](../../deploy-and-configure/integrations/index.md).

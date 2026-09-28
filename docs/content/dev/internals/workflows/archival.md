!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Archive Workflow

CTA validates the request, creates archive jobs, transfers file data, and reports completion to the disk system.

## CTA implementation

See [Workflow Frontend Internals](../components/workflow-api.md) and [Scheduling Workflow](scheduling.md). TODO: Describe the disk-independent request path, implementation entry points, and state transitions.

## Disk-system integration

See [EOS Archive Workflow](../../guides/integrations/eos/archival.md) for the EOS-specific protocol and behaviour.

## Failure handling and tests

TODO: Document retries, cancellation, partial completion, recovery after process failure, and the tests covering these paths.

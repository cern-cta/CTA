!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Readiness and Liveness

This page covers service readiness/liveness endpoints and their implementation. These checks expose basic process status; logs, metrics, and request state provide the detail needed for investigation.

## Readiness and liveness

TODO: Explain how service state determines readiness and liveness, including temporary dependency failures and startup/shutdown transitions.

## Implementation

TODO: Document registering and updating checks through the service runtime. Keep deployment settings in [Operations Health Checks](../../ops/run-and-maintain/monitoring/health-and-alerts.md).

## Testing

TODO: Cover healthy operation, unavailable dependencies, recovery, and shutdown without relying on production monitoring infrastructure.

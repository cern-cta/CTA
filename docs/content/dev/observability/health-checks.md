# Health Checks

!!! info "Documentation outline"
    Detailed implementation guidance will be added during the content review.

## Readiness and liveness

Explain how service state determines readiness and liveness, including temporary dependency failures and startup/shutdown transitions.

## Implementation

Document registering and updating checks through the service runtime. Keep deployment settings in [Operations Health Checks](../../ops/monitoring/health-and-alerts.md).

## Testing

Cover healthy operation, unavailable dependencies, recovery, and shutdown without relying on production monitoring infrastructure.

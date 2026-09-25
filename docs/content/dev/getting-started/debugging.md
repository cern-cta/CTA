# Debugging

!!! info "Documentation outline"
    Detailed procedures will be completed and validated during the content review.

Start with the [debugging environment workflow](development-workflow.md#build-a-debugging-environment) to prepare a development instance.

## Reproduce and isolate

Document reproducing failures with targeted tests and identifying the responsible service, request, and scheduler backend. See [Testing CTA](../testing/index.md).

## Debuggers and crash investigation

Document debug symbols, attaching to a service, breakpoints, stack traces, core dumps, and preserving the matching binaries and configuration.

## Runtime diagnostics

Document selecting logging and sanitizer options and investigating hangs or resource problems. See [Logging and Monitoring](../observability/index.md).

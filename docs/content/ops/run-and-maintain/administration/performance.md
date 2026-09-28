!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Performance and Capacity

Operator guidance for sizing and tuning CTA.

## Measure the bottleneck

TODO: Document throughput, queue age, mount utilisation, and disk-transfer measurements.

## Tune CTA

TODO: Document drive allocation, scheduling policies, buffers, and reporting batch sizes.

## Disk buffer capacity

TODO: Document how disk capacity and transfer rates constrain tape activity; put system-specific tuning under the corresponding integration.

## Retrieval ordering and disk-buffer tuning

Use [RAO configuration](../../deploy-and-configure/configuration/tape-daemon.md#recommended-access-order) for tape positioning and [EOS performance and disk layout](../../deploy-and-configure/integrations/eos/performance.md) for EOS-specific limits and buffer policies.

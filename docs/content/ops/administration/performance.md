# Performance and Capacity

Operator guidance for sizing and tuning CTA.

!!! info "Documentation outline"
    The sections below reserve space for the detailed documentation to be added.

## Measure the bottleneck

Document throughput, queue age, mount utilisation, and disk-transfer measurements.

## Tune CTA

Document drive allocation, scheduling policies, buffers, and reporting batch sizes.

## Disk buffer capacity

Document how disk capacity and transfer rates constrain tape activity; put system-specific tuning under the corresponding integration.

## Retrieval ordering and disk-buffer tuning

Use [RAO configuration](../configuration/tape-daemon.md#recommended-access-order) for tape positioning and [EOS performance and disk layout](../integrations/eos/performance.md) for EOS-specific limits and buffer policies.

!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Performance and Disk Layout

EOS-specific layout and transfer policies for the CTA disk buffer. See also [CTA performance and capacity](../../../run-and-maintain/administration/performance.md).

## Spaces, layouts, and tape replicas

TODO: Explain EOS space selection, disk layout and replica counts, and the meaning of a tape replica in the namespace. Review layout-policy mechanisms before adding configuration examples.

## Rate limiting and starvation

TODO: Document applicable transfer limits, their scope, and how to diagnose starvation. Use measurements from the deployment rather than historical site-specific rates.

## Buffer sizing and cleanup

Distinguish cache retention from transfer-buffer capacity. Connect pressure and throughput monitoring to [Buffer Cleanup](buffer-cleanup.md).

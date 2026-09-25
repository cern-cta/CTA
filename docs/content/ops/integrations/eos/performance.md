# Performance and Disk Layout

EOS-specific layout and transfer policies for the CTA disk buffer. See also [CTA performance and capacity](../../administration/performance.md).

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

## Spaces, layouts, and tape replicas

Explain EOS space selection, disk layout and replica counts, and the meaning of a tape replica in the namespace. Review layout-policy mechanisms before adding configuration examples.

## Rate limiting and starvation

Document applicable transfer limits, their scope, and how to diagnose starvation. Use measurements from the deployment rather than historical site-specific rates.

## Buffer sizing and cleanup

Distinguish cache retention from transfer-buffer capacity. Connect pressure and throughput monitoring to [Buffer Cleanup](buffer-cleanup.md).

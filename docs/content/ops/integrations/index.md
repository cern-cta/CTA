# Disk Buffer Integration

CTA manages tape copies, tape hardware, and tape scheduling. The disk system manages the client-facing namespace, disk replicas, and client requests. Install and configure these systems separately, then verify their integration.

## Shared setup

Configure the [Workflow API](../configuration/workflow-api.md) and [Admin API](../configuration/admin-api.md), [authentication](../configuration/authentication.md), and [disk-instance and storage policies](../administration/storage-policies.md). See [Disk Buffer Concepts](../../concepts/components/disk-buffer.md) for the responsibility boundary.

## EOS

- [Installation](eos/installation.md)
- [Configuration](eos/configuration.md)
- [File Attributes](eos/file-attributes.md)
- [Metadata Consistency and Recovery](eos/metadata-recovery.md)
- [Tape REST API](eos/tape-rest-api.md)
- [Troubleshooting and Repair](eos/troubleshooting.md)
- [Performance and Disk Layout](eos/performance.md)
- [Buffer Cleanup](eos/buffer-cleanup.md)
- [Upgrades](../upgrades/eos.md)
- [Operator Utilities](eos/tools.md)

## dCache

The [dCache Integration](dcache.md) page reserves the corresponding operator documentation. Its presence is an outline, not a claim that deployment instructions are complete.

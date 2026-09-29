# Disk System Integration

CTA manages tape copies, tape hardware, and tape scheduling. The disk system manages the client-facing namespace, disk replicas, and client requests. Install and configure these systems separately, then verify their integration.

## Shared setup

Configure the [Workflow Frontend](../configuration/workflow-frontend.md) and [Admin Frontend](../configuration/admin-frontend.md), [authentication](../configuration/authentication.md), and [disk-instance and storage policies](../../run-and-maintain/administration/storage-policies.md). See [Disk System Concepts](../../../concepts/components/disk-system.md) for the responsibility boundary.

## EOS

| Task | Start here |
| --- | --- |
| Setup: install EOS | [Installation](eos.md#installation) |
| Setup: connect EOS to CTA | [Configuration](eos.md#configuration) |
| Operation: tune layout and throughput | [Performance & Disk Layout](../../run-and-maintain/administration/eos/performance.md) |
| Operation: manage buffer space | [Buffer Cleanup](../../run-and-maintain/administration/eos/buffer-cleanup.md) |
| Operation: upgrade EOS | [Upgrading Disk System: EOS](../../run-and-maintain/upgrades/disk-system.md#eos) |
| Reference: inspect workflow metadata | [EOS File Attributes](../../tools/eos-file-attributes.md) |
| Reference: use EOS operator commands | [cta-ops-eos](../../tools/cta-ops-eos.md) |

## dCache

Use [dCache Integration](dcache.md) for upstream documentation and CTA plugin references. Contributions to the CTA-specific dCache guidance are welcome.

| Task | Start here |
| --- | --- |
| Find installation, configuration, and administration guidance | [dCache Integration](dcache.md) |
| Upgrade dCache | [Upgrading Disk System: dCache](../../run-and-maintain/upgrades/disk-system.md#dcache) |

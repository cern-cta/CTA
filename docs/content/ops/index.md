---
title: Operations introduction
---

# Operations

This section is for people installing, configuring, running, and maintaining CTA. It covers core CTA services and their integration with a separately managed disk system. Development documentation is not a prerequisite for operating CTA.

## Deploy & Configure

Begin with [Deployment Planning](deploy-and-configure/deployment/planning.md), [Installation](deploy-and-configure/deployment/installation/index.md), and [Initialisation and Verification](deploy-and-configure/deployment/initialisation.md). Use [Service Runtime](deploy-and-configure/configuration/service-runtime.md) for configuration and choose the relevant [Disk Buffer Integration](deploy-and-configure/integrations/index.md). EOS-specific instructions do not apply automatically to other disk systems. [Concepts](../concepts/index.md) explains the shared terminology and architecture.

## Run & Maintain

Use [Administration](run-and-maintain/administration/index.md) for routine tasks, [Monitoring](run-and-maintain/monitoring/health-and-alerts.md) to assess service health, and [Upgrading CTA](run-and-maintain/upgrades/cta.md) for upgrades.

## Troubleshooting & Recovery

Start with [Troubleshooting](troubleshooting-and-recovery/troubleshooting.md). Use [Backup & Recovery](troubleshooting-and-recovery/backup-and-recovery.md) to restore deployment state or [Recycle Bin & File Recovery](troubleshooting-and-recovery/file-recovery.md) to recover files. Disk-system-specific procedures remain in [EOS Troubleshooting & Repair](deploy-and-configure/integrations/eos/troubleshooting.md), [EOS Metadata Consistency & Recovery](deploy-and-configure/integrations/eos/metadata-recovery.md), and the [dCache integration guide](deploy-and-configure/integrations/dcache.md).

## Tools & Reference

Use the [Tool Index](tools/index.md) to find command and service references. See also [example configurations](deploy-and-configure/configuration/service-runtime.md#component-examples) and the [Community Forum](https://cta-community.web.cern.ch).

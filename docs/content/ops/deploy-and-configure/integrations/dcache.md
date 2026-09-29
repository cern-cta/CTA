# dCache Integration

dCache integrates with CTA through the [dCache CTA nearline-storage plugin](https://github.com/dCache/dcache-cta). Use the upstream documentation for deployment and operation:

| Topic | Documentation |
| --- | --- |
| dCache installation, configuration, and administration | [dCache documentation](https://www.dcache.org/documentation/) — select the administrator guide for your release. |
| CTA plugin installation and configuration | [dCache CTA plugin README](https://github.com/dCache/dcache-cta#readme) |
| dCache upgrades | [Upgrading Disk System](../../run-and-maintain/upgrades/disk-system.md#dcache) |

Check the plugin's requirements against the dCache and CTA versions being deployed. CTA's [Workflow Frontend](../configuration/workflow-frontend.md) and [authentication](../configuration/authentication.md) documentation describe the CTA side of the integration.

!!! note "Contributions welcome"

    CTA-specific operational documentation for dCache is limited. Contributions from external operators and developers running dCache with CTA are welcome, particularly validated setup, verification, monitoring, and troubleshooting procedures.

For a development test instance, see [Developing with dCache](../../../dev/guides/integrations/dcache.md).

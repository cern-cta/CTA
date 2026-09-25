# Installation Methods

Choose how to install CTA services after reviewing [Deployment Planning](../planning.md). Different services may use different methods: tape services need access to the tape hardware, while frontend and maintenance services can be placed separately.

| Method | Documentation |
| --- | --- |
| RPM packages | [Install and manage RPM packages](rpm-packages.md). |
| Docker images | [Deploy published images](docker-images.md); the operator procedure is being documented. |

## After installation

Supply and validate [service configuration](../../configuration/service-runtime.md), configure the selected [disk-system integration](../../integrations/index.md), and follow [Initialisation and Verification](../initialisation.md). These steps are shared across installation methods.

For hosts connected to tape hardware, follow [Tape Server Setup](../tape-servers.md) before commissioning drives.

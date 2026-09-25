# EOS Installation

Install EOS packages only on the hosts that need the EOS server or client. Core CTA installation is documented [separately](../../deployment/installation/index.md).

The `cta-release` package supplies repository definitions and dependency locks for the corresponding release. Select the EOS packages appropriate to each host:

```shell
dnf install eos-server
dnf install eos-client
```

Continue with [EOS Configuration](configuration.md). EOS dependencies and compatibility must be checked for the chosen deployment.

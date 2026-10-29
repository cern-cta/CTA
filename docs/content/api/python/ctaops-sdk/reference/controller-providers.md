# Controller providers

Controller providers are the recommended entry points: they enumerate visible hardware and return controller instances that share a single pyudev `Context`.

## Provider protocols

::: ctaops.sdk.controller_providers.protocols.drive

::: ctaops.sdk.controller_providers.protocols.library

## SCSI-backed implementations

::: ctaops.sdk.controller_providers.scsi.drive

::: ctaops.sdk.controller_providers.scsi.library

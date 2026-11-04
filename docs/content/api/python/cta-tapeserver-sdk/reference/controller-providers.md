# Controller providers

Controller providers are the recommended entry points: they enumerate visible hardware and return controller instances that share a single pyudev `Context`.

## Provider protocols

::: cta.tapeserver.sdk.controller_providers.protocols.drive

::: cta.tapeserver.sdk.controller_providers.protocols.library

## SCSI-backed implementations

::: cta.tapeserver.sdk.controller_providers.scsi.drive

::: cta.tapeserver.sdk.controller_providers.scsi.library

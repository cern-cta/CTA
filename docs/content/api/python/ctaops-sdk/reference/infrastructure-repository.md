# Repositories

The repositories layer enumerates locally-visible tape drives and libraries in a read-only fashion. Two pairs of modules expose it:

- The **protocols** package describes the contract and the immutable
  value objects returned by repository methods.
- The **local** package contains the udev-backed implementations the SDK
  ships with.

## Drive repository protocols and value objects

::: ctaops.sdk.repositories.protocols.drive

## Library repository protocols and value objects

::: ctaops.sdk.repositories.protocols.library

## Local (udev-backed) implementations

### Drives

::: ctaops.sdk.repositories.local.drive

### Libraries

::: ctaops.sdk.repositories.local.library

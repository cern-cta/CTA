# `cta-objectstore-initialize`

Initialise a new scheduler objectstore with its root entry and required registers.

Available in objectstore scheduler builds, with `CTA_USE_PGSCHED` disabled.

## Usage

```text
cta-objectstore-initialize [objectstoreURL]
```

Without a URL, the command creates a VFS backend and prints its location. This differs from commands that read the default scheduler configuration.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

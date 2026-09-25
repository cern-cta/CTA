# `cta-objectstore-list`

List object names in the scheduler objectstore.

Available in objectstore scheduler builds, with `CTA_USE_PGSCHED` disabled.

## Usage

```text
cta-objectstore-list [objectstoreURL]
```

Without a URL, the command reads `/etc/cta/cta-scheduler.conf`.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

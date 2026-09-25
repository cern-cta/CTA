# `cta-objectstore-collect-orphaned-object`

Garbage-collect an orphaned scheduler object. This can modify or delete scheduler data.

Available in objectstore scheduler builds, with `CTA_USE_PGSCHED` disabled.

## Usage

```text
cta-objectstore-collect-orphaned-object [objectstoreURL catalogueLoginFile] objectname
```

With only an object name, the command reads `/etc/cta/cta-scheduler.conf` and `/etc/cta/cta-catalogue.conf`.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

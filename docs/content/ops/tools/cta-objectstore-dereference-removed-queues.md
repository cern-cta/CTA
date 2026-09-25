# `cta-objectstore-dereference-removed-queues`

Remove references to missing archive and retrieve queues from the objectstore root entry. This changes scheduler metadata.

Available in objectstore scheduler builds, with `CTA_USE_PGSCHED` disabled.

## Usage

```text
cta-objectstore-dereference-removed-queues [objectstoreURL]
```

Without a URL, the command reads `/etc/cta/cta-scheduler.conf`.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

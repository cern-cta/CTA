# `cta-objectstore-create-missing-repack-index`

Recreate the repack index in the objectstore root entry. The implementation attempts to remove the existing index before creating a replacement.

Available in objectstore scheduler builds, with `CTA_USE_PGSCHED` disabled.

## Usage

```text
cta-objectstore-create-missing-repack-index [objectstoreURL]
```

Without a URL, the command reads `/etc/cta/cta-scheduler.conf`.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

# `cta-objectstore-reset`

Delete every object in the specified scheduler objectstore. This is a destructive operation; the command requires the literal confirmation `yes` on standard input.

Available in objectstore scheduler builds, with `CTA_USE_PGSCHED` disabled.

## Usage

```text
cta-objectstore-reset objectstoreURL
```

The URL is required. Reset does not initialise a replacement scheduler store.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

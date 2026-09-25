# `cta-scheduler-schema-drop`

Drop the PostgreSQL scheduler schema. This removes scheduler database structures and data.

Available in builds configured with `CTA_USE_PGSCHED`.

## Usage

```text
cta-scheduler-schema-drop databaseConnectionFile [options]
```

The connection file identifies the scheduler database. `-h` / `--help` prints usage.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

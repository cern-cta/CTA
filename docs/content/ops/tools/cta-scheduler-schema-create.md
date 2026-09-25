# `cta-scheduler-schema-create`

Create the PostgreSQL scheduler schema.

Available in builds configured with `CTA_USE_PGSCHED`.

## Usage

```text
cta-scheduler-schema-create databaseConnectionFile [options]
```

The connection file identifies the scheduler database. `-h` / `--help` prints usage.

`-v` / `--version` selects the scheduler schema version to create.

See [Scheduler Configuration](../configuration/scheduler.md) for backend setup.

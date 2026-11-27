# Testing with External Databases

Use `ci/orchestration/create_instance.sh` to connect a [development instance](../../getting-started/environment-setup.md) to an external catalogue, scheduler, or both.
The examples below use PostgreSQL and require images built with `--scheduler-type pgsched`.
For an Oracle catalogue, use Oracle-enabled images and an Oracle connection string instead.

## Connection Secrets

Create Kubernetes Secrets in the `default` namespace with the following keys and connection strings.
The Secret names are arbitrary; the keys must match exactly.
Example values below are plain text (`stringData` in a Secret manifest).

| Secret name | Key | Example value |
| --- | --- | --- |
| `external-catalogue` | `cta-catalogue.conf` | `postgresql:postgresql://catalogue_user:password@catalogue.example.org:5432/cta_catalogue` |
| `external-scheduler` | `cta-scheduler.conf` | `postgresql:postgresql://scheduler_user:password@scheduler.example.org:5432/cta_scheduler` |

The first `postgresql:` selects CTA's database driver; the rest is the PostgreSQL connection URI.

## Deploy

Run from `ci/orchestration/`, using a new deployment namespace and a matching image tag:

```bash
./create_instance.sh --namespace external-test \
  --cta-image-tag <image-tag> \
  --catalogue-secret external-catalogue \
  --scheduler-secret external-scheduler \
  --cta-values presets/cta/scheduler/postgres.yaml
```

The connection options copy the Secrets into the deployment namespace and skip the corresponding default local provisioning.
The scheduler values preset selects the backend; a Secret alone does not select it.

`create_instance.sh` does not reset databases by default.
Use initialized databases, or add `--reset-catalogue` and/or `--reset-scheduler` to erase and initialize the selected connections.
For deployment through `cta-dev`, see its [connection and reset options](cta-dev.md#connect-to-an-external-database).

## Use only one external database

Keep the PostgreSQL scheduler values preset and replace the two connection options above with:

| External connection | Options |
| --- | --- |
| Catalogue | `--catalogue-secret external-catalogue --infra-values presets/infra/scheduler-postgres.yaml --reset-scheduler` |
| Scheduler | `--scheduler-secret external-scheduler --reset-catalogue` |

The explicit infrastructure preset selects a local PostgreSQL scheduler instead of the default VFS storage.
The two connections' state must be compatible, particularly when connecting an existing scheduler to a fresh catalogue.

## Testing and cleanup

Use databases dedicated to this instance: services and tests can modify their contents even without deployment resets.
See [System Tests](../testing/system-tests.md#lifecycle-and-cleanup) for test setup and teardown, and [Working with Development Pods](development-pods.md) for inspection commands.
Deleting the deployment namespace does not delete the source Secrets in `default` or the external databases.

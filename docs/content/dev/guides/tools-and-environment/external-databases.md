# Testing with External Databases

Use `cta-dev` with local builds, or `ci/orchestration/create_instance.sh` with externally built images, to connect a [development instance](../../getting-started/environment-setup.md) to an external catalogue, scheduler, or both.
The examples below use PostgreSQL and require images built with `--scheduler-type pgsched`.
For an Oracle catalogue, use Oracle-enabled images and an Oracle connection string instead.

## Connection Secrets

The connection Secrets must exist in the `default` namespace.
Their names are arbitrary; the keys must be exactly `cta-catalogue.conf` and `cta-scheduler.conf`.

`kubectl -n default get secret external-catalogue -o yaml` should look like this, omitting server-generated metadata:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: external-catalogue
  namespace: default
type: Opaque
data:
  cta-catalogue.conf: <base64-encoded catalogue connection string>
```

Likewise, `kubectl -n default get secret external-scheduler -o yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: external-scheduler
  namespace: default
type: Opaque
data:
  cta-scheduler.conf: <base64-encoded scheduler connection string>
```

Each decoded value is a CTA connection string, for example `postgresql:postgresql://<user>:<password>@<host>:5432/<database>`.
The first `postgresql:` selects CTA's database driver; the rest is the PostgreSQL connection URI.

## Deploy

Both tools copy the selected Secrets into the deployment namespace and skip the corresponding default local provisioning and resets.
Use initialized databases, or add `--reset-catalogue` or `--reset-scheduler` to erase and initialize the selected connection.

### Local builds with cta-dev

Build and deploy from your checkout:

```bash
cta-dev up --namespace external-test --scheduler-type pgsched \
  --catalogue-secret external-catalogue \
  --scheduler-secret external-scheduler
```

Use `cta-dev deploy` with the same options to reuse existing local images.
See [cta-dev deployment options](cta-dev.md#deployment-options) for other settings.

### External images with create_instance.sh

Run from `ci/orchestration/`, using a new deployment namespace and a matching image tag:

```bash
./create_instance.sh --namespace external-test \
  --cta-image-tag <image-tag> \
  --catalogue-secret external-catalogue \
  --scheduler-secret external-scheduler \
  --cta-values presets/cta/scheduler/postgres.yaml
```

The scheduler values preset selects the backend; a Secret alone does not select it.

## Use only one external database

Keep the scheduler option or values preset and replace the two connection options in the respective command with:

| External connection | cta-dev options | create_instance.sh options |
| --- | --- | --- |
| Catalogue | `--catalogue-secret external-catalogue` | `--catalogue-secret external-catalogue --infra-values presets/infra/scheduler-postgres.yaml --reset-scheduler` |
| Scheduler | `--scheduler-secret external-scheduler` | `--scheduler-secret external-scheduler --reset-catalogue` |

`cta-dev` provisions and resets the local connection automatically.
`create_instance.sh` requires explicit reset options; its default local scheduler uses VFS storage, so the catalogue-only example also selects PostgreSQL provisioning.
The two connections' state must be compatible, particularly when connecting an existing scheduler to a fresh catalogue.

## Testing and cleanup

Use databases dedicated to this instance: services and tests can modify their contents even without deployment resets.
See [System Tests](../testing/system-tests.md#lifecycle-and-cleanup) for test setup and teardown, and [Working with Development Pods](development-pods.md) for inspection commands.
Deleting the deployment namespace does not delete the source Secrets in `default` or the external databases.

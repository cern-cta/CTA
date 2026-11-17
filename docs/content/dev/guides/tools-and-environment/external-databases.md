# Testing with External Databases

Use `cta-dev` to run a development instance against your own PostgreSQL catalogue and scheduler databases.
The examples invoke `./ci/cta-dev.sh` from the checkout root; installing the `cta-dev` command is optional.

## Requirements

- A prepared [development environment](../../getting-started/environment-setup.md), including Kubernetes, Helm and virtual tape hardware.
- Two initialized PostgreSQL databases reachable from the Pods, with compatible catalogue and scheduler state and schemas matching your CTA images.
- Images built with `--scheduler-type pgsched`; see [Building Images & Packages](building-images-and-packages.md) to build your own.

Use databases dedicated to this instance and ensure their tape, library and disk-system configuration matches the environment.

!!! warning "Protection is not read-only access"

    The examples prevent automatic bootstrap resets and test setup/teardown.
    Services, ordinary state-changing tests and manual commands can still modify database state.
    A Kubernetes namespace does not isolate databases shared with another CTA instance.

## Configure the connections

Keep the connection details outside the checkout in a private directory:

```bash
mkdir -p "$HOME/cta-external"
chmod 700 "$HOME/cta-external"
```

Create two files with your connection details:

| File under `$HOME/cta-external/` | Example contents |
| --- | --- |
| `cta-catalogue.conf` | `postgresql:postgresql://catalogue_user:CHANGE_ME@catalogue.example.org:5432/cta_catalogue` |
| `cta-scheduler.conf` | `postgresql:postgresql://scheduler_user:CHANGE_ME@scheduler.example.org:5432/cta_scheduler` |

The first `postgresql:` selects CTA's database driver; the remaining `postgresql://...` is the PostgreSQL connection URI.
Replace the example credentials and hosts, URI-encode special characters, and add any required TLS options.
Create the Secrets in `default`, which survives recreation of the deployment namespace:

```bash
chmod 600 "$HOME/cta-external/cta-catalogue.conf" "$HOME/cta-external/cta-scheduler.conf"
kubectl -n default create secret generic external-catalogue \
  --from-file=cta-catalogue.conf="$HOME/cta-external/cta-catalogue.conf"
kubectl -n default create secret generic external-scheduler \
  --from-file=cta-scheduler.conf="$HOME/cta-external/cta-scheduler.conf"
```

Keep the files outside version control.
The requested Secrets are copied during deployment; their contents do not pass through Helm values.

Create `$HOME/cta-external/cta.yaml`:

```yaml
global:
  catalogue:
    existingSecret: external-catalogue
    protected: true
  scheduler:
    existingSecret: external-scheduler
    protected: true
```

The Secret names must match the manually created Secrets.
The keys inside those Secrets must remain `cta-catalogue.conf` and `cta-scheduler.conf`.

## Deploy and verify

Replace `<image-tag>` with a published PostgreSQL scheduler image tag:

```bash
./ci/cta-dev.sh deploy --namespace external-test \
  --scheduler-type pgsched --cta-image-tag <image-tag> \
  --copy-secret external-catalogue --copy-secret external-scheduler \
  --infra-values presets/infra/external.yaml \
  --cta-values "$HOME/cta-external/cta.yaml"
```

`--copy-secret` checks that each source Secret is readable before replacing the deployment, then copies it into the new namespace.
`--infra-values presets/infra/external.yaml` disables local catalogue and scheduler provisioning.
Other development infrastructure is still deployed.
The command replaces any existing CTA-managed deployment in `external-test`, without resetting or migrating the protected databases.
Use the same values file and copy options when redeploying.
For local images and registry overrides, see [Deployment options](cta-dev.md#deployment-options).

Check the workloads, protection policy and catalogue connection:

```bash
kubectl -n external-test get pods
kubectl -n external-test get configmap cta-connection-protection -o yaml
kubectl -n external-test exec deployment/cta-tools-admin -c admin -- \
  cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
kubectl -n external-test exec cta-cli-0 -- cta-admin version
```

Both protection entries should be `"true"`.
Schema verification checks catalogue compatibility; `cta-admin version` checks administrative frontend communication.
For logs and troubleshooting, see [Working with Development Pods](development-pods.md).

## Use only one external database

Omit the local connection's `--copy-secret` option and remove its entire override from `cta.yaml`.
Replace `--infra-values presets/infra/external.yaml` with the appropriate argument:

| External connection | Replacement argument |
| --- | --- |
| Catalogue | `--infra-values presets/infra/scheduler-postgres.yaml` |
| Scheduler | `--infra-values presets/infra/catalogue-postgres.yaml` |

The local database is provisioned and reset during bootstrap; the external connection remains protected.
Their state must still be compatible, particularly when connecting an existing scheduler to a fresh catalogue.

## Run automated tests

Keep protection enabled when testing against an existing database or production clone:

```bash
./ci/cta-dev.sh test --namespace external-test client
```

Automatic setup and teardown are skipped; selected tests and verification still run.
The environment must already contain the resources expected by the selected tests.
To authorize a schema migration test explicitly:

```bash
./ci/cta-dev.sh test --namespace external-test catalogue_schema_update --allow-destructive
```

The migration suite expects the catalogue's starting version to match the oldest supported version and performs update and rollback.
Permission does not enable automatic teardown or remove protection.
Request destructive cleanup separately with `test --namespace external-test teardown --allow-destructive` only when you intend to discard the catalogue contents.
See [System Tests](../testing/system-tests.md#protected-and-external-connections) for selection and lifecycle options.

## Clean up

```bash
kubectl delete namespace external-test
```

This removes the development workloads, copied Secrets and Helm release records.
The source Secrets in `default` remain available for redeployment; delete them separately when no longer needed.
It does not delete the external databases or undo changes made by CTA; manage their lifecycle separately.

# Testing with External Databases

Use `ci/orchestration/create_instance.sh` to run an instance against your own catalogue, scheduler, or both.
This guide assumes a PostgreSQL scheduler; the catalogue can use PostgreSQL or Oracle.
Run the deployment command from `ci/orchestration/`.

## Requirements

- A prepared [development environment](../../getting-started/environment-setup.md), including Kubernetes, Helm and virtual tape hardware.
- An initialized external catalogue or scheduler database, or both, reachable from the Pods and compatible with your CTA images and the other connection's state.
- Images built with `--scheduler-type pgsched`; see [Building Images & Packages](building-images-and-packages.md) to build your own.

For an Oracle catalogue, use Oracle-enabled images and an Oracle catalogue connection file.
The connection examples below use PostgreSQL for both databases; see [Use only one external database](#use-only-one-external-database) to override just one connection.

Use databases dedicated to this instance and ensure their tape, library and disk-system configuration matches the environment.

!!! warning "Existing databases remain writable"

    The deployment examples do not request resets of external databases.
    Services, system tests and manual commands can still modify or delete database state.
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

The keys inside those Secrets must remain `cta-catalogue.conf` and `cta-scheduler.conf`.

## Deploy and verify

Replace `<image-tag>` with a published PostgreSQL scheduler image tag:

```bash
cd ci/orchestration
./create_instance.sh --namespace external-test \
  --cta-image-tag <image-tag> \
  --catalogue-secret external-catalogue \
  --scheduler-secret external-scheduler \
  --cta-values presets/cta/scheduler/postgres.yaml
```

The connection options select the Secrets, require them to be readable in `default`, and copy them into the new namespace.
They override connection Secret names in CTA values and omit the corresponding default local database or storage provisioning.
The scheduler backend is selected separately through CTA values and must match the images.
Other development infrastructure is still deployed.
Use a new namespace; the script does not replace an existing deployment and rejects an existing CTA Helm release.
Use `--cta-image-registry` if the images are not in the registry configured by `project.json`.

No catalogue or scheduler reset is requested by default, regardless of whether the connections are local or external.
For CI databases that must be initialized/reset, add `--reset-catalogue`, `--reset-scheduler`, or both.
These options operate on the selected connections without a separate protection policy or permission override.
Use the same connection options when redeploying after removing the old deployment.

Check the workloads, reset selection and catalogue connection:

```bash
kubectl -n external-test get pods
helm -n external-test get values cta-tools
kubectl -n external-test exec deployment/cta-tools-backend -c backend -- \
  cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf
kubectl -n external-test exec deployment/cta-tools-cli -- cta-admin version
```

`reset.catalogue` and `reset.scheduler` should both be `false` unless you explicitly requested those resets.
Schema verification checks catalogue compatibility; `cta-admin version` checks administrative frontend communication.
For logs and troubleshooting, see [Working with Development Pods](development-pods.md).

## Use only one external database

Keep the PostgreSQL scheduler values preset and replace the two connection options in the deployment command as follows:

| External connection | Connection and initialization options |
| --- | --- |
| Catalogue | `--catalogue-secret external-catalogue --infra-values presets/infra/scheduler-postgres.yaml --reset-scheduler` |
| Scheduler | `--scheduler-secret external-scheduler --reset-catalogue` |

Only the locally provisioned database is reset in these examples.
The explicit infrastructure preset in the first example selects a local PostgreSQL scheduler instead of the default VFS storage.
Explicit `--infra-values` files control provisioning independently of the connection flags and replace all default infrastructure presets.
The two connections' state must still be compatible, particularly when connecting an existing scheduler to a fresh catalogue.

## Run automated tests

Use disposable databases for system tests.
Deployment reset options do not control the test runner: `cta-dev test` and the test phase of `cta-dev all` include setup and teardown, which can delete existing state.
See [Lifecycle and cleanup](../testing/system-tests.md#lifecycle-and-cleanup) for the phases and [Run pytest directly](../testing/system-tests.md#run-pytest-directly) for explicit selection.
Omitting setup and teardown does not prevent the selected tests or their fixtures from modifying existing data.

## Clean up

```bash
kubectl delete namespace external-test
```

This removes the development workloads, copied Secrets and Helm release records.
The source Secrets in `default` remain available for redeployment; delete them separately when no longer needed.
It does not delete the external databases or undo changes made by CTA; manage their lifecycle separately.

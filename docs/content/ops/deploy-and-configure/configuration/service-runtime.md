# Service Runtime

Configure service validation and runtime metadata independently of the installation method. See the individual service references for supported options and [example configurations](service-runtime.md#component-examples).

## Strict Config Checking

These options apply to services using the CTA Runtime Library. Check the service option reference for availability.

When defining a config file for a service, there are multiple problems that can occur:

1. An operator can make a typo in the configuration option, causing them to think something is configured, but in reality the option is ignored.
2. Software may change option names between (major) releases, causing previously working options to break (transparently).
3. Software may change default values of config options, causing a change in behaviour despite the operator never changing the config file.

Services that use the CTA Runtime Library support the `--config-strict` flag which can prevent all of these. Concretely it enforces the following rules:

- Every provided config value MUST be a valid config entry. That means it will fail when entries are present that are not consumed by CTA. This mitigates (1) and (2).
- Every applicable setting MUST be explicit, including settings with defaults. Genuinely optional values and sections for disabled features may be absent. This mitigates (2) and (3).

Settings that support automatic sizing or system defaults still require an explicit value in strict mode, such as `grpc.number_of_threads = 0`.
Consult each setting's description for the meaning of zero and other special values.

It is possible to check the validity of a config beforehand using the `--config-check` flag, which can be combined with the `--config-strict` flag. This checks the configuration and exits without starting the service.
`--config-check` cannot be combined with `--runtime-dir`, so checking configuration does not overwrite a service's runtime snapshots.

It is highly recommended to use `--config-strict` when deploying CTA to ensure operators have full control and transparency over the consumed config values.

## Using the example configurations

The packaged examples are recommended starting configurations.
Replace deployment-specific names, endpoints and credential paths, and select the features your service needs.

Field comments distinguish three cases:

| Metadata | Meaning when the section is active |
| --- | --- |
| `default: <value>` | May be omitted in non-strict mode; must be supplied in strict mode. |
| `default: none; required` | Must be supplied in either mode. |
| `default: none; optional` | May be omitted in either mode; the description explains the effect. |

Select service-wide capabilities through root `enabled_features` and `enabled_experimental_features` lists.
Telemetry and PostgreSQL workflow batching are experimental; their interfaces may change or be removed.
Strict mode requires both lists, including `[]` when empty; non-strict mode defaults both to empty.
Supply settings for selected features and remove sections for disabled features, including their child tables.
Unknown names, duplicates and selections in the wrong list are rejected.

To disable a feature, remove its name from the appropriate list and remove its configuration section.
For example, disabling telemetry requires removing `"telemetry"` from `enabled_experimental_features` and removing `[telemetry]`.
See [Metrics](../../run-and-maintain/monitoring/metrics.md) and [Health and Alerts](../../run-and-maintain/monitoring/health-and-alerts.md) for complete activation examples.

Nested selectors work independently: authentication uses `auth.enabled_methods`, maintenance uses `routines.enabled_routines`, and tape transfers use `transfers.encryption_enabled` and `transfers.retrieve.rao_enabled`.
Their sections must also match the selections in both modes.
In non-strict mode, maintenance defaults to all supported routines with their default settings, and both APIs default to JWT authentication, requiring `[auth.jwt]`.
In strict mode, supply each selector and its selected sections explicitly.

## Environment Variables

Services using the CTA Runtime Library expand explicit environment references before parsing and validating the main TOML configuration.
This allows multiple processes to share a configuration file while supplying different values through their environment.

- `${NAME}` inserts the environment variable's value. An undefined variable becomes an empty string.
- `${NAME:-fallback}` uses the fallback when the variable is undefined or empty.
- `$$` inserts a literal dollar: `$${NAME}` produces the literal text `${NAME}`. Bare `$NAME` is not expanded.

For example, a string value uses `name = "${CTA_NAME}"`, while an integer value uses `port = ${CTA_PORT:-1234}`.
Replacement values and fallbacks are not expanded recursively.
Invalid substitution references cause an error.

Substitution applies throughout the file, including keys and comments, and inserts values verbatim.
Values must therefore be valid in their TOML context, including any required quoting or escaping.
The substitution syntax is based on OpenTelemetry and does not support prefixes, but this whole-file preprocessing does not prevent values from changing the TOML structure.

`--config-strict` checks the resulting configuration; it does not make undefined environment variables an error by themselves.
An undefined variable inside quotes can produce a valid empty string.

## Runtime Directory

Services that use the CTA Runtime Library have the option to populate an existing directory with runtime metadata:

- **`config.toml`**: the original main configuration supplied through `--config <path>`, before environment expansion.
- **`config.resolved.toml`**: the exact text parsed after environment expansion, preserving comments and formatting.
- **`cta-logging.schema.json`**: a JSON schema for the logs. See [Logging](../../run-and-maintain/monitoring/logging.md).
- **`catalogue.config_file`**: the file containing the catalogue connection string. Note that the name of the file reflects the full TOML path to ensure this can be correlated with the corresponding option in the config file.
- **`scheduler.config_file`**: the file containing the scheduler connection string. Its name reflects the full TOML path of the option. Only exists when CTA is compiled with the Postgres scheduler.
- **`telemetry.config_file`**: the file containing the declarative telemetry configuration. Its name reflects the full TOML path of the option.
- **`version.json`**: a simple JSON file containing the name of the service and its version. Example:

    ```json
    {"service": "cta-maintd", "version": "<installed-cta-version>"}
    ```

The runtime directory contains a snapshot of all files the service read at startup. Changes to configuration files on disk do not take effect until the service restarts, and configuration management systems may not trigger a restart automatically. The runtime directory allows operators to compare the intended configuration (e.g. the file specified by `--config <path>`) with the snapshot (e.g. `<runtimedir>/config.toml`) to identify changes that have not yet taken effect.

Compare the configured source file with `config.toml` to detect changes since startup.
Compare `config.toml` with `config.resolved.toml` to see the environment variable substitutions (if any) used at startup.

To enable this feature, use the `--runtime-dir <path>` flag. The lifecycle of the directory itself is not managed by the CTA process. The directory must exist before the process starts and must be readable and writable by the service. The process does not clean it up. The deployment layer manages it as follows:

- **Systemd RPM deployment**: The packaged unit uses `RuntimeDirectory=` to manage the directory lifecycle. When running multiple processes on the same host, ensure the directory name is unique per unit. See [RPM Packages and Services](../deployment/installation/rpm-packages.md#log-and-runtime-directories).
- **Kubernetes**: Simply create a directory as an `emptyDir` volume. Because the lifetime of the service is tied to the lifetime of the pod, there should be no issues with name clashes.

## Component examples

Example files are included with the component they configure:

- [Workflow Frontend](workflow-frontend.md#example-configuration)
- [Admin Frontend](admin-frontend.md#example-configuration)
- [Tape Daemon](tape-daemon.md#example-configuration)
- [Maintenance Daemon](maintenance-daemon.md#example-configuration)
- [Media Changer Daemon](media-changer-daemon.md#example-configuration)
- [Catalogue](catalogue.md#example-configuration)
- [Scheduler](scheduler.md#objectstore-connection-example)
- [Telemetry](../../run-and-maintain/monitoring/metrics.md#example-configuration)

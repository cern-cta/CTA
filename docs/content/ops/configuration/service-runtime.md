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
- Every single config value parsed by CTA MUST be present in the config file. As a result, changes in default values will not affect the deployment, because every value is explicit and defaults are no longer relied upon. This mitigates (2) and (3).

It is possible to check the validity of a config beforehand using the `--config-check` flag, which can be combined with the `--config-strict` flag. This checks the configuration and exits without starting the service.

It is highly recommended to use `--config-strict` when deploying CTA to ensure operators have full control and transparency over the consumed config values.

## Runtime Directory

Services that use the CTA Runtime Library have the option to populate an existing directory with runtime metadata.:

- **`config.toml`**: the main config file of the service. This is the same file as passed in the `--config <path>` flag.
- **`cta-logging.schema.json`**: a JSON schema for the logs. See [Logging](../monitoring/logging.md).
- **`catalogue.config_file`**: the file containing the catalogue connection string. Note that the name of the file reflects the full TOML path to ensure this can be correlated with the corresponding option in the config file.
- **`scheduler.config_file`**: the file containing the scheduler connection string. Its name reflects the full TOML path of the option. Only exists when CTA is compiled with the Postgres scheduler.
- **`telemetry.config_file`**: the file containing the declarative telemetry configuration. Its name reflects the full TOML path of the option.
- **`version.json`**: a simple JSON file containing the name of the service and its version. Example:

    ```json
    {"service": "cta-maintd", "version": "<installed-cta-version>"}
    ```

The runtime directory provides a snapshot of all files consumed by the service when it started. Typically, changing the config file on disk will not automatically restart a service, which means that your service may be running with a different config file than what you may expect. As such, the runtime directory allows operators to detect inconsistencies between the intended configuration file (e.g. the config path used in `--config <path>`) and the files actually consumed by the service (e.g. `<runtimedir>/config.toml`).

To enable this feature, use the `--runtime-dir <path>` flag. The lifecycle of the directory itself is not managed by the CTA process. The directory must exist before the process starts and must be readable and writable by the service. The process does not clean it up. The deployment layer manages it as follows:

- **Systemd RPM deployment**: The packaged unit uses `RuntimeDirectory=` to manage the directory lifecycle. When running multiple processes on the same host, ensure the directory name is unique per unit. See [RPM Packages and Services](../deployment/installation/rpm-packages.md#log-and-runtime-directories).
- **Kubernetes**: Simply create a directory as an `emptyDir` volume. Because the lifetime of the service is tied to the lifetime of the pod, there should be no issues with name clashes.

## Component examples

Example files are included with the component they configure:

- [Workflow API](workflow-api.md#example-configuration)
- [Admin API](admin-api.md#example-configuration)
- [Tape Daemon](tape-daemon.md#example-configuration)
- [Maintenance Daemon](maintenance-daemon.md#example-configuration)
- [Media Changer Daemon](media-changer-daemon.md#example-configuration)
- [Catalogue](catalogue.md#example-configuration)
- [Scheduler](scheduler.md#objectstore-connection-example)
- [Telemetry](../monitoring/metrics.md#example-configuration)

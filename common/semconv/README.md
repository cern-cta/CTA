# Semantic Conventions

This directory contains a set of constants for attribute names used in CTA.
All semantic conventions follow those established by OpenTelemetry where possible. OpenTelemetry has been designed such that its concepts and naming work for many different systems.

- These constants MUST follow the conventions as detailed here: <https://opentelemetry.io/docs/specs/semconv/general/naming/>.
- See <https://opentelemetry.io/docs/specs/semconv/> for existing semantic conventions.

While the telemetry SDK exposes some of these values, this could potentially cause name changes when the SDK is upgraded. As such, we explicitly define the keys we use to ensure consistency.
Additionally, this allows us to have this set of constants in a single place and it also makes sense for components not using telemetry.

## Variable naming

The constants in this directory can define both the name of the keys AND names of the possible values.

### Attribute Keys

The variable names for attribute keys MUST be the same as their values with the following modifications:

- Prefixed with a "k"
- CamelCasing instead of the dot separator

For example, a constant value of `service.instance.id` should be exposed by a variable called `kServiceInstanceId`.

### Attribute Values

The variable names for attribute values MUST be the same as their values with the following modifications:

- Prefixed with a "k"
- CamelCasing instead of the dot separator

In addition, values MUST live in a separate namespace specific to the attribute key that they are for.
The namespace name MUST be the attribute key expressed using CamelCasing, appended by `Values`.

For example, the attribute key `cta.transfer.direction` might have as a possible value `archive`.

- The attribute key variable would be `cta::semconv::attr::kCtaIoDirection`.
- The attribute value variable would be `cta::semconv::attr:CtaTransferDirectionValues::kArchive`

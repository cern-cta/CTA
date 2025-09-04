# Semantic Conventions

This directory contains a set of constants for attribute names used in CTA.
These constants MUST follow the conventions as detailed here: https://opentelemetry.io/docs/specs/semconv/general/naming/.

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

The variable names for the attribute values MUST be a combination of the variable name of the attribute key and the value itself.

For example, the attribute key `transfer.direction` might have as a possible value `read`. In this case, the variable for the attribute value would be `kTransferDirectionRetrieve`.

# Semantic conventions

Shared constants for CTA instrumentation. Defining the keys here keeps emitted names stable across telemetry SDK upgrades and makes them available to components that do not use the SDK directly.

Use the [semantic conventions](../../docs/content/dev/guides/instrumentation/semantic-conventions.md) for names and values, the [C++ instrumentation guide](../../docs/content/dev/guides/instrumentation/cpp.md) for their use, and the [logging conventions](../../docs/content/dev/guides/instrumentation/logging.md) for log-schema compatibility.

## Attribute naming quick reference

Keep shared definitions in `Attributes.hpp` rather than repeating string literals in callers. Reuse OpenTelemetry names where applicable.

| Definition | Naming rule | Example |
| --- | --- | --- |
| Attribute key | Prefix with `k` and convert the dotted key to CamelCase. | `service.instance.id` → `kServiceInstanceId` |
| Attribute value | Prefix with `k` and convert the value to CamelCase. | `archive` → `kArchive` |
| Value namespace | Convert the attribute key to CamelCase and append `Values`. | `cta.transfer.direction` → `CtaTransferDirectionValues` |

Value constants must live in the namespace for their attribute key. For example:

```cpp
cta::semconv::attr::kCtaTransferDirection                 // "cta.transfer.direction"
cta::semconv::attr::CtaTransferDirectionValues::kArchive  // "archive"
```

These rules apply to attribute constants in `Attributes.hpp`. For the existing log-field naming in `Logging.hpp`, follow the linked logging conventions.

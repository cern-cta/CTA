# Semantic conventions

Shared constants for CTA instrumentation. Defining the keys here keeps emitted names stable across telemetry SDK upgrades and makes them available to components that do not use the SDK directly.

Use the [semantic conventions](../../docs/content/dev/guides/instrumentation/semantic-conventions.md) for names and values, the [C++ instrumentation guide](../../docs/content/dev/guides/instrumentation/cpp.md) for their use, and the [logging conventions](../../docs/content/dev/guides/instrumentation/logging.md) for log-schema compatibility.

For example, `cta::semconv::attr::kCtaTransferDirection` names `cta.transfer.direction`, and `cta::semconv::attr::CtaTransferDirectionValues::kArchive` supplies its archive value. Keep shared definitions here rather than repeating string literals in callers.

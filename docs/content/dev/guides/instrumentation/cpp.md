# C++ Instrumentation

Use this guide to implement the shared [logging](logging.md) and [metrics](metrics.md) conventions in C++ components.

## Logging

CTA's logging framework lives in `common/log/`. Use the `cta::log::LogContext` supplied by the calling code so request and service context follows the operation.

### Emit a log message

Add operation-specific attributes with `ScopedParamContainer` and emit the message through the context:

```cpp
#include "common/log/Constants.hpp"
#include "common/log/LogContext.hpp"
#include "common/semconv/Logging.hpp"

void reportFailure(cta::log::LogContext& lc, const std::string& reason) {
  cta::log::ScopedParamContainer params(lc);
  params.add(cta::semconv::log::errorMessage, reason);
  lc.log(cta::log::ERR, "Failed to complete the operation");
}
```

Use a specific operation name in real code. Attributes added by the container apply to logs emitted through that context until the container leaves scope, when they are removed and any outer values restored. Keep the scope as small as the operation requires. Do not share mutable contexts between concurrent operations without an explicit synchronization strategy.

Use `lc.logEvent(level, message, eventName)` for a named event, reusing constants in `common/semconv/Logging.hpp`. Source location is captured by the logging API; callers normally do not need to provide it.

### Severity and attribute constants

Use the levels in `common/log/Constants.hpp`: `DEBUG`, `INFO`, `WARNING`, and `ERR`, with `CRIT`, `ALERT`, and `EMERG` reserved for the corresponding service-impacting conditions. CTA maps user errors to `USERERR`, an alias of `NOTICE`.

Reuse log-attribute and event constants from `common/semconv/Logging.hpp`. These currently use underscore-separated field names; do not silently change existing keys to match dotted metric names.

### Test logging

Use `cta::log::StringLogger` with a `LogContext` to capture output in C++ tests; see `common/log/StringLoggerTest.cpp` and `LogContextTest.cpp` for examples. Check meaningful message content, severity filtering, required attributes, and scoped-context cleanup. Avoid matching an entire rendered line containing timestamps or source locations.

## Metrics

Follow [Metrics Conventions](semantic-conventions.md) when defining the measurement contract.

### Define constants

Add the metric's name, description, and unit to `common/semconv/Metrics.hpp`. Add attribute constants to `common/semconv/Attributes.hpp` and reuse the component's meter from `common/semconv/Meter.hpp`.

Attribute-key constants use a `k` prefix and CamelCase derived from the dotted key, such as `kServiceInstanceId` for `service.instance.id`. Group predefined attribute values in a namespace for their key.

### Declare and initialize the instrument

Component instruments live in `lib/telemetry/include/telemetry/metrics/` and `lib/telemetry/src/metrics/`. The existing maintenance-routine duration histogram illustrates the pattern. Its header declares:

```cpp
namespace cta::telemetry::metrics {
extern std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaMaintdRoutineDuration;
}
```

The corresponding source file defines and registers it:

```cpp
#include "telemetry/metrics/MaintdMetrics.hpp"
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"
#include "telemetry/metrics/InstrumentRegistry.hpp"
#include "telemetry/metrics/MetricsUtils.hpp"
#include "version.hpp"

namespace cta::telemetry::metrics {
std::unique_ptr<opentelemetry::metrics::Histogram<uint64_t>> ctaMaintdRoutineDuration;
}

namespace {
void initInstruments() {
  auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaMaintd, CTA_VERSION);
  cta::telemetry::metrics::ctaMaintdRoutineDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaMaintdRoutineDuration,
                                cta::semconv::metrics::descrCtaMaintdRoutineDuration,
                                cta::semconv::metrics::unitCtaMaintdRoutineDuration);
}

const auto registrar = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}
```

The header includes the OpenTelemetry instrument types. Extend the existing component files where possible. If adding a component, update `lib/telemetry/CMakeLists.txt` and link the instrument library into its consumers.

The registrar initially creates instruments against the default provider and registers the function for reinitialization when telemetry is configured. Keep registration in the source file, not the header. Use the shared instrument rather than caching its pointer in individual objects; initialization can replace it. Provider lifecycle belongs in [Telemetry Internals](../../internals/telemetry.md).

### Record measurements

Include the component header and record at the intended operation boundary. For the histogram above, the unit is milliseconds:

```cpp
#include "common/semconv/Attributes.hpp"
#include "telemetry/metrics/MaintdMetrics.hpp"

// elapsedMilliseconds is measured for one routine execution.
cta::telemetry::metrics::ctaMaintdRoutineDuration->Record(
  elapsedMilliseconds,
  {{cta::semconv::attr::kCtaRoutineName, routineName}},
  opentelemetry::context::Context {});
```

Here `routineName` must come from the bounded set of routine types, not an execution-specific identifier. Counters use `Add(...)` rather than `Record(...)`.

Avoid expensive attribute construction or additional I/O in frequently executed paths. Check that retries, failure paths, and nested callers do not accidentally record the same event twice. Finish by [testing the metric](testing.md).

### Inspect exported metrics

For direct inspection, the legacy telemetry configuration offers `STDOUT` and `FILE` debug exporters as well as `NOOP` and OTLP backends. Use the configuration supported by the service you are testing; configuration interfaces differ during the runtime migration. These debug exporters are for development, not production collection.


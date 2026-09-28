---
title: OpenTelemetry metrics
---

# OpenTelemetry Metrics in CTA

In CTA, we use OpenTelemetry to produce [Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/) that allow us to gain valuable insights into the availability and performance of the system. Metrics are measurements captured at runtime that are produced by [instruments](https://opentelemetry.io/docs/concepts/instrumentation/). The moment of capturing a measurement is known as a metric event, which consists not only of the measurement itself, but also the time at which it was captured and associated metadata[^1]. This associated metadata comes in the form of attributes, similar to how a log entry has additional context besides just the message.

Attributes are useful to gain additional understanding about a measurement. Attributes can be per-metric, but they can also be at the [Resource](https://opentelemetry.io/docs/concepts/resources/) level. A resource is an entity that produces metrics. Some examples of attributes at the resource level could be a process name, a container name, a pod name, a namespace and a deployment name. These all help to identify the entity producing certain metrics. For example, if you are seeing long latencies on particular requests, it is useful to understand the entity (process/container/host) responsible for this.

[^1]: <https://opentelemetry.io/docs/concepts/signals/metrics/>

## Instrument Kinds

A metric instrument is something that captures measurements and therefore produces metrics. We will refer to this simply as an "instrument" in the remainder of this page.
Different instrument kinds exist:

- Counters: A value that accumulates over time, but can only go up.
- UpDownCounters: A value that accumulates over time, but can also go down again.
- Histograms: A client-side aggregation of values, such as request latencies.
- Gauge: Measures a current value at the time it is read (think e.g. temperature or CPU usage %).

For a more detailed and complete list of the instrument kinds, see <https://opentelemetry.io/docs/concepts/signals/metrics/#metric-instruments>.

## Adding new metrics

Telemetry in CTA has been set up in such a way that it should be easy to add new metrics (see [Design Decisions](#design-decisions)).
However, adding the metric itself is the easy part. The more tricky part comes from deciding _when_ to instrument and _what_ to instrument.
The steps required for adding a new metric are detailed below.

### 0. Deciding what to instrument

Instrumentation is a very powerful tool to gain insights in the performance and usage of an application. However, with great power comes great responsibility.
You should careful consider the following:

- If your measurement requires high cardinality to be useful, it cannot be instrumented.
    - Example: tagging every metric with a unique `vid` or `query_text` would create millions of distinct time series.
- While the performance impact of telemetry is generally minimal, adding an instrument to something that is called tens of thousands of times per second may negatively impact performance.
- If the metric won’t drive a decision or alert, it may add noise rather than value.
- If the data only makes sense per individual event and loses meaning when aggregated (e.g., exact stack traces, raw error strings, mount IDs), a metric isn’t the right tool.

We also highly advise you to read the following short section on when not to instrument from the official docs:

- <https://opentelemetry.io/docs/concepts/instrumentation/libraries/#when-not-to-instrument>

### 1. Picking the correct instrument

To prevent repeating the official OpenTelemetry documentation, please read through the following section:

- <https://opentelemetry.io/docs/specs/otel/metrics/supplementary-guidelines/#guidelines-for-instrumentation-library-authors>

### 2. Naming your metric

The name, description and unit of your metric should go into `common/semconv/Metrics.hpp`. Before you do this, first check whether your instrument already fits into the [established semantic conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/). If so, use those conventions (along with any relevant attributes). If not, define your own name, closely following the conventions of CTA and OpenTelemetry. Do not invent your own instrument/attribute names without cross-checking these with existing OpenTelemetry conventions.

### 3. Defining the instrument

Instruments are defined as global variables in the `cta::telemetry::metrics` namespace. Generally, all instrument for a given library reside in the same file. For now, all instruments reside in `common/telemetry/metrics/instruments/`.

The header file should contain the instrument declaration. For example, for the `taped` instrumentation, we would _declare_ the following in `TapedInstruments.hpp`:

```c++
namespace cta::telemetry::metrics {

/**
 * Number of files transferred to/from tape.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferCount;

/**
 * Bytes transferred to/from tape.
 */
extern std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferIO;

}  // namespace cta::telemetry::metrics
```

Then in `TapedInstruments.cpp`, we need to _define_ these variables:

```c++
namespace cta::telemetry::metrics {

std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferCount;
std::unique_ptr<opentelemetry::metrics::Counter<uint64_t>> ctaTapedTransferIO;

}  // namespace cta::telemetry::metrics
```

The header uses extern to _declare_ the variables, while the `.cpp` file provides their single _definition_. This is intentional: it lets any translation unit that includes the header refer to the same variables without creating multiple definitions.

However, as you might notice, we did not initialise the instrument yet. This is done in an `initInstruments()` function residing in an anonymous namespace:

```c++
#include "common/semconv/Meter.hpp"
#include "common/semconv/Metrics.hpp"

namespace {
void initInstruments() {
   auto meter = cta::telemetry::metrics::getMeter(cta::semconv::meter::kCtaTaped, CTA_VERSION);

  // Note the usage of the semantic convention constants here
  cta::telemetry::metrics::ctaTapedTransferCount =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaTapedTransferCount,
                               cta::semconv::metrics::descrCtaTapedTransferCount,
                               cta::semconv::metrics::unitCtaTapedTransferCount);

  cta::telemetry::metrics::ctaTapedTransferIO =
    meter->CreateUInt64Counter(cta::semconv::metrics::kMetricCtaTapedTransferIO,
                               cta::semconv::metrics::descrCtaTapedTransferIO,
                               cta::semconv::metrics::unitCtaTapedTransferIO);

  cta::telemetry::metrics::ctaTapedMountDuration =
    meter->CreateUInt64Histogram(cta::semconv::metrics::kMetricCtaTapedMountDuration,
                                 cta::semconv::metrics::descrCtaTapedMountDuration,
                                 cta::semconv::metrics::unitCtaTapedMountDuration);
}

// Register and run this init function at start time
const auto _ = cta::telemetry::metrics::InstrumentRegistrar(initInstruments);
}  // namespace
```

To briefly explain the important parts here. First, we initialise the [meter](https://opentelemetry.io/docs/concepts/signals/metrics/#meter) for the library. A meter is what initialises instruments and is specific to a given library/component and version (of said library).

This meter is used to initialise the global instruments, which typically take a name, description and unit.

Finally, we create the instrument registrar as a static variable in the anonymous namespace. This ensures that the `initInstruments` function is registered at program start time so that it can be called when telemetry is initialised.

### 4. Using the instrument

To use the instrument, simply import the relevant `*Instruments.hpp` file and use the `->Add(..)`/`->Record(..)` methods from the instruments themselves to capture your measurements.

### 5. Adding attributes to your instrument recordings

Adding metric-level attributes can help with investigating metrics. However, adding attributes also increases the cardinality of your metric.
**Controlling cardinality is crucial**.

To a degree, high cardinality is unavoidable in an environment where many services are deployed. For the majority of time-series, it is desirable to be able to identify a given service. For example, in the future our CTA deployment might have O(1000) processes running, which means that a single instrument can already produce a similar number of time series.

Prometheus recommends to keep the cardinality of metrics below 10. **As such, for any attribute added to a metric, its number of possible values must be very small**. The majority of metrics should have no additional attributes. Remember that the resource attributes already contain a lot of information (such as Catalogue/Scheduler info), so it is not necessary to add these as metric attributes.

If your instrument is one already defined by the [established semantic conventions](https://opentelemetry.io/docs/specs/semconv/general/metrics/), it is typically desirable to include the required attributes as defined there.

Attribute names are part of the semantic conventions and should therefore go in `common/semconv/Attributes.hpp`.

References:

- <https://grafana.com/blog/2022/10/20/how-to-manage-high-cardinality-metrics-in-prometheus-and-kubernetes/>
- <https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels>
- <https://prometheus.io/docs/practices/instrumentation/#use-labels>

---

## Design Decisions

Due to the complexity of OpenTelemetry, there are various design decisions that were made when implementing this. Below, we list the most important ones.
Please read through these carefully before making any changes to the way telemetry was set up.

### Cumulative vs Delta Metrics

There are two types of [temporality](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#temporality) in OpenTelemetry metrics: cumulative and delta.

- Cumulative: Each data point represents the total value of a metric since the start of measurement (or the last reset). For example, a counter measuring HTTP requests will always increase over time until it resets.
- Delta: Each data point represents the change since the last collection interval. Using the same example, the metric would report only the number of HTTP requests received in the most recent collection period.

It is recommended to use the same temporality as the one used by the backend system:

- Prometheus (our target time-series database) expects cumulative metrics. Therefore, CTA will emit cumulative metrics by default.
    - Once all services support the [declarative configuration](https://opentelemetry.io/docs/languages/sdk-configuration/declarative-configuration/) this can be changed by operators to suit their needs.
    - If delta metrics are required (e.g., for integration with other systems or processors), the OpenTelemetry Collector can convert cumulative metrics to delta using a processor.

References:

- <https://opentelemetry.io/docs/specs/otel/metrics/data-model/#temporality>
- <https://grafana.com/blog/2023/09/26/opentelemetry-metrics-a-guide-to-delta-vs.-cumulative-temporality-trade-offs/>

### Uniquely Identifiable Processes

Every service/process must be uniquely identifiable, see [Single-Writer](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#single-writer). Failing to satisfy this constraint can lead to data loss and/or incorrect metrics.

In CTA, we deal with (1) by having each process include a globally unique uuid in the `service.instance.id` resource attribute, as recommended by [the docs](https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/). This guarantees that each process is uniquely identifiable. However, this presents issues with cardinality: each time `cta-taped` forks we generate a new `service.instance.id`. To combat this, we provide the option to use a deterministic `service.instance.id`, comprised of the host name and the process title.

This ensures that we do not create new time series every time `cta-taped` forks. Things such as counter resets can be detected by the Observability backend to due [a change in start time](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#resets-and-gaps). Once forking is removed from taped, it can follow the same strategy as the other services.

### Instrument Reuse

Duplicate instrument registrations are considered semantic errors, see [Producer Recommendations](https://opentelemetry.io/docs/specs/otel/metrics/data-model/#opentelemetry-protocol-data-model-producer-recommendations). For example, if an instrument is initialised in the constructor of a class and two instances of said class are created, we are dealing with a semantic error. Similar to the single writer principle, failing to satisfy this constraint can lead to data loss and/or incorrect metrics.

To tackle this, we need a way to prevent duplicate instrument registrations. Two approaches were considered for this: a caching mechanism and defining all instruments as global variables. Ultimately, CTA uses global variables to keep track of its instruments, as this approach is simpler, faster and less error-prone. A caching mechanism has several disadvantages:

- It would require locks to ensure thread-safety when it comes to initialisation. Seeing as instruments can be called extremely frequently, this would hurt performance.
- It might entice developers to "cache" the instruments by defining them as class members to overcome the locking contention. However, this can lead to unexpected errors when it comes to (re)initialisation of the instruments.
- It means the instrument definitions would be spread out all over the code, making it difficult to find which instruments exist and what attributes they use.
- Instruments need to be defined in multiple places (wherever they are used), which can lead to semantic errors if they don't have the exact same properties.

The global variable approach handles this a lot better:

- No thread safety is required when it comes to initialisation. Note that instrument _usage_ is already thread-safe by default.
- No need for caching, as it is already possible to directly access the instrument.
- All instrument definitions can be done in a single place, making it easy to find which instruments are being used.

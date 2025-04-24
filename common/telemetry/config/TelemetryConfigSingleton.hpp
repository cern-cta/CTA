#include "TelemetryConfig.hpp"

namespace cta::telemetry {

/**
 * Used for ensuring initMetrics can be called from anywhere without passing the MetricsConfig along everywhere
 * Must be initialised once before using
 */
class TelemetryConfigSingleton {
public:
  static void initialize(TelemetryConfig config) { getMutableInstance() = std::move(config); }

  static const TelemetryConfig& get() { return getMutableInstance(); }

private:
  static TelemetryConfig& getMutableInstance() {
    static TelemetryConfig instance;
    return instance;
  }
};

}  // namespace cta::telemetry

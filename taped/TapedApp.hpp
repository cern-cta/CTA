/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SchedulerContext.hpp"
#include "TapeDaemon.hpp"
#include "TapedConfig.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"

#include <atomic>
#include <map>
#include <memory>
#include <vector>

namespace cta::tape::daemon {

/// @brief Own the tape daemon and its runtime dependencies.
///
/// Run once; stop and health callbacks may run concurrently with run(), but must end before destruction.
class TapedApp final {
public:
  /// Create an application without starting a tape daemon.
  TapedApp() = default;

  /// Destroy the daemon and its dependencies, then shut down the Protobuf library.
  ~TapedApp();

  /// Request exit from a published daemon.
  void stop();

  /// @brief Initialize and run the daemon, returning its exit code.
  ///
  /// Configuration and logger must outlive the application; initialization exceptions propagate to the runtime.
  int run(const TapedConfig& config, cta::log::Logger& log);

  /// Return the configured drive and logical library as static log attributes.
  std::map<std::string, std::string> getStaticLogAttributes(const TapedConfig& config) const;

  /// Return the configured drive and logical library as static telemetry attributes.
  std::map<std::string, std::string> getStaticTelemetryAttributes(const TapedConfig& config) const;

  /// Return daemon liveness, or true before the daemon is published.
  bool isLive() const;

  /// Return daemon readiness, or false before the daemon is published.
  bool isReady() const;

private:
  /// Declaration order keeps dependencies alive until their borrowers are destroyed.
  std::unique_ptr<catalogue::Catalogue> m_catalogue;
  std::unique_ptr<SchedulerContext> m_schedulerContext;
  std::unique_ptr<TapeDaemon> m_tapeDaemon = nullptr;
  /// Non-owning publication of m_tapeDaemon for concurrent stop and health callbacks.
  std::atomic<TapeDaemon*> m_publishedDaemon {nullptr};
};

}  // namespace cta::tape::daemon

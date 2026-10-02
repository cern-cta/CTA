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

/// Entrypoint for cta-taped.
class TapedApp final {
public:
  /// Create an application without starting a tape daemon.
  TapedApp() = default;

  /// Destroy the daemon and its dependencies before shutting down the Protocol Buffers library.
  ~TapedApp();

  /// Request exit from a published daemon.
  void stop();

  /// @brief Set the process name, enable core dumps where possible, construct and run the daemon.
  ///
  /// Configuration and logger must outlive the application.
  /// @return Daemon exit code; initialization exceptions propagate to the runtime.
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

/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveController.hpp"
#include "TapedConfig.hpp"
#include "common/log/LogContext.hpp"

#include <atomic>
#include <map>
#include <vector>

namespace cta::tape::daemon {

class TapedApp final {
public:
  /**
   * @brief Create an application without starting a drive controller.
   */
  TapedApp() = default;

  /**
   * @brief Destroy the controller before shutting down the Protocol Buffers library.
   */
  ~TapedApp();

  /**
   * @brief Request controller exit after its current operation or tape session.
   */
  void stop();

  /**
   * @brief Enable core dumping where possible, create the controller and run it.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @param log Logger used for diagnostics; it must outlive objects retaining a reference to it.
   * @return The controller exit code; initialization exceptions propagate to the runtime.
   */
  int run(const TapedConfig& config, cta::log::Logger& log);

  /**
   * @brief Return the configured drive name as a static log attribute.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @return Drive-name attribute to attach to log entries.
   */
  std::map<std::string, std::string> getStaticLogAttributes(const TapedConfig& config) const;

  /**
   * @brief Return the drive and logical library names as static telemetry attributes.
   *
   * @param config Daemon configuration; borrowed configuration must outlive the owning object.
   * @return Drive-name and logical-library attributes to attach to telemetry.
   */
  std::map<std::string, std::string> getStaticTelemetryAttributes(const TapedConfig& config) const;

  /**
   * @brief Return whether taped is alive (stuck) or not.
   *
   * @return True when the application or controller considers itself live.
   */
  bool isLive() const;

  /**
   * @brief Return whether taped is ready to do work or not.
   *
   * @return True when the application or controller considers itself ready.
   */
  bool isReady() const;

private:
  std::unique_ptr<DriveController> m_driveController = nullptr;
  // Health callbacks run concurrently with controller construction.
  std::atomic<DriveController*> m_publishedController {nullptr};
};

}  // namespace cta::tape::daemon

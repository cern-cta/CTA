/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DriveHandler.hpp"
#include "TapedConfig.hpp"
#include "common/log/LogContext.hpp"

#include <map>
#include <vector>

namespace cta::tape::daemon {

class TapedApp final {
public:
  TapedApp() = default;

  ~TapedApp() = default;

  void stop();

  int run(const TapedConfig& config, cta::log::Logger& log);

  std::map<std::string, std::string> getStaticLogAttributes(const TapedConfig& config) const;

  std::map<std::string, std::string> getStaticTelemetryAttributes(const TapedConfig& config) const;

  bool isLive() const;

  bool isReady() const;

private:
  std::unique_ptr<DriveHandler> m_driveHandler = nullptr;
};

}  // namespace cta::tape::daemon

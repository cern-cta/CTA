/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedApp.hpp"

#include "TapedUtils.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/utils.hpp"

#include <google/protobuf/stubs/common.h>
#include <sys/prctl.h>

namespace cta::tape::daemon {

void TapedApp::stop() {
  if (m_driveHandler) {
    m_driveHandler->stop();
  }
}

std::map<std::string, std::string> TapedApp::getStaticLogAttributes(const TapedConfig& config) const {
  return {
    {"drive_name", config.drive.name}
  };
}

std::map<std::string, std::string> TapedApp::getStaticTelemetryAttributes(const TapedConfig& config) const {
  return {
    {cta::semconv::attr::kTapeDriveName,          config.drive.name                },
    {cta::semconv::attr::kTapeLibraryLogicalName, config.drive.logical_library_name}
  };
}

int TapedApp::run(const TapedConfig& config, cta::log::Logger& log) {
  log::LogContext lc(log);

  // Linux may mark the process non-dumpable when messing with capabilities in certain cases. To be safe, we explicitly enable it.
  // See https://man7.org/linux/man-pages/man2/pr_set_dumpable.2const.html
  cta::utils::setDumpableProcessAttribute(true);
  if (!cta::utils::getDumpableProcessAttribute()) {
    log(log::WARNING, "Failed to set the dumpable attribute. Core dumps may not be produced");
  }

  // Run the main part of taped
  m_driveHandler = std::make_unique<DriveHandler>(config, log);
  return m_driveHandler->run();
}

bool TapedApp::isReady() const {
  return m_driveHandler && m_driveHandler->isReady();
}

bool TapedApp::isLive() const {
  if (!m_driveHandler) {
    // We consider ourselves alive if we haven't started yet, because a restart likely won't fix this.
    return true;
  }
  return m_driveHandler->isLive();
}

}  // namespace cta::tape::daemon

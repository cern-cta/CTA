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

TapedApp::~TapedApp() {
  m_driveController.reset();
  google::protobuf::ShutdownProtobufLibrary();
}

void TapedApp::stop() {
  if (auto* controller = m_publishedController.load()) {
    controller->stop();
  }
}

std::map<std::string, std::string> TapedApp::getStaticLogAttributes(const TapedConfig& config) const {
  return {
    {"drive_name",     config.drive.name                },
    {"logicalLibrary", config.drive.logical_library_name}  // The casing is just for backward compatibility
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
  // TODO: we should still set a recognisable process name

  // Linux may mark the process non-dumpable when messing with capabilities in certain cases. To be safe, we explicitly enable it.
  // See https://man7.org/linux/man-pages/man2/pr_set_dumpable.2const.html
  cta::utils::setDumpableProcessAttribute(true);
  if (!cta::utils::getDumpableProcessAttribute()) {
    log(log::WARNING, "Failed to set the dumpable attribute. Core dumps may not be produced");
  }

  // Run the main part of taped
  m_driveController = std::make_unique<DriveController>(config, log);
  m_publishedController.store(m_driveController.get());
  return m_driveController->run();
}

bool TapedApp::isReady() const {
  const auto* controller = m_publishedController.load();
  return controller && controller->isReady();
}

bool TapedApp::isLive() const {
  const auto* controller = m_publishedController.load();
  if (!controller) {
    // We consider ourselves alive if we haven't started yet, because a restart likely won't fix this.
    return true;
  }
  return controller->isLive();
}

}  // namespace cta::tape::daemon

/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedApp.hpp"

#include "daemon/DriveHandler.hpp"
#include "daemon/ProcessManager.hpp"

namespace cta::tape::daemon {

void TapedApp::stop() {}

int TapedApp::run(const TapedConfig& config, cta::log::Logger& log) {
  // Create the log context
  log::LogContext lc(m_log);
  // Set process name
  const auto processName = m_globalConfiguration.constructProcessName(lc, "parent");
  prctl(PR_SET_NAME, processName.c_str());

  // TODO: telemetry
  // TODO: signal handling
  // Create the process manager and signal handler
  ProcessManager processManager(lc);
  // Create the drive handler
  const common::dataStructures::DriveInfo driveInfo(config.drive.name,
                                                    utils::getShortHostname(),
                                                    config.drive.logical_library_name,
                                                    config.drive.device,
                                                    config.drive.control_path);
  auto driveHandler = std::make_unique<DriveHandler>(config, driveInfo, processManager);
  processManager.addHandler(std::move(driveHandler));

  // And run the process manager
  int ret = processManager.run();
  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}

bool TapedApp::isReady() const {
  return true;  // TODO
}

bool TapedApp::isLive() const {
  return true;
}

}  // namespace cta::tape::daemon

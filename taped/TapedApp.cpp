/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedApp.hpp"

#include "TapedUtils.hpp"
#include "daemon/DriveHandler.hpp"
#include "daemon/ProcessManager.hpp"
#include "daemon/SignalHandler.hpp"

#include <google/protobuf/stubs/common.h>
#include <sys/prctl.h>

namespace cta::tape::daemon {

void TapedApp::stop() {}

int TapedApp::run(const TapedConfig& config, cta::log::Logger& log) {
  // TODO: add some non-empty checks on the config

  // Create the log context
  log::LogContext lc(log);
  // Set process name
  const auto processName = cta::taped::utils::constructProcessName(config.drive.name, "parent", lc);
  prctl(PR_SET_NAME, processName.c_str());

  // TODO: telemetry
  // Create the process manager
  ProcessManager processManager(lc);
  // Signal handler
  auto signalHandler = std::make_unique<SignalHandler>(processManager);
  // Allow enough time to return the tape to its library slot and finish shutdown bookkeeping.
  signalHandler->setTimeout(std::chrono::seconds(config.mounts.unmount_timeout_secs + 5));
  processManager.addHandler(std::move(signalHandler));
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
  return true;  // For now taped is always ready. Eventually we should integrate this with the existing watch dog
}

bool TapedApp::isLive() const {
  return true;
}

}  // namespace cta::tape::daemon

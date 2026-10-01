/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedApp.hpp"

#include "SystemDriveOperations.hpp"
#include "TapedUtils.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/utils.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <google/protobuf/stubs/common.h>
#include <sys/prctl.h>

namespace cta::tape::daemon {

TapedApp::~TapedApp() {
  m_tapeDaemon.reset();
  m_driveOperations.reset();
  google::protobuf::ShutdownProtobufLibrary();
}

void TapedApp::stop() {
  if (auto* daemon = m_publishedDaemon.load()) {
    daemon->stop();
  }
}

std::map<std::string, std::string> TapedApp::getStaticLogAttributes(const TapedConfig& config) const {
  return {
    {"tapeDrive",      config.drive.name                },
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

  // Observe drive state while taped runs; the runtime has already initialized telemetry.
  telemetry::metrics::ScopedTapedStateMetrics stateMetrics;

  // Run the main part of taped
  m_driveOperations = makeSystemDriveOperations(config, log);
  m_tapeDaemon = std::make_unique<TapeDaemon>(config, log, *m_driveOperations);
  m_publishedDaemon.store(m_tapeDaemon.get());
  return m_tapeDaemon->run();
}

bool TapedApp::isReady() const {
  const auto* daemon = m_publishedDaemon.load();
  return daemon && daemon->isReady();
}

bool TapedApp::isLive() const {
  const auto* daemon = m_publishedDaemon.load();
  if (!daemon) {
    // We consider ourselves alive if we haven't started yet, because a restart likely won't fix this.
    return true;
  }
  return daemon->isLive();
}

}  // namespace cta::tape::daemon

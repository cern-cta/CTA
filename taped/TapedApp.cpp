/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedApp.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <cerrno>
#include <google/protobuf/stubs/common.h>
#include <string>
#include <sys/prctl.h>
#include <system_error>

namespace cta::tape::daemon {

namespace {

std::string constructProcessName(const std::string& driveName, log::LogContext& lc) {
  // Linux allows 15 name bytes; reserve six for the "taped-" prefix.
  constexpr std::size_t maxShortNameLength = 9;
  const auto pos = driveName.find_last_of('-');
  auto shortName = pos == std::string::npos ? driveName : driveName.substr(pos + 1);
  if (shortName.size() > maxShortNameLength) {
    lc.log(log::WARNING, "Short drive name '" + shortName + "' exceeds 9 bytes; truncating process name");
    shortName.resize(maxShortNameLength);
  }
  return "taped-" + shortName;
}

}  // namespace

TapedApp::~TapedApp() {
  m_tapeDaemon.reset();
  m_schedulerContext.reset();
  m_catalogue.reset();
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
  const auto processName = constructProcessName(config.drive.name, lc);
  if (::prctl(PR_SET_NAME, processName.c_str(), 0UL, 0UL, 0UL) == -1) {
    const int error = errno;
    log::ScopedParamContainer params(lc);
    params.add("processName", processName)
      .add("errorMessage", std::error_code(error, std::generic_category()).message());
    lc.log(log::WARNING, "Failed to set process name");
  }

  // Linux may mark the process non-dumpable when messing with capabilities in certain cases. To be safe, we explicitly enable it.
  // See https://man7.org/linux/man-pages/man2/pr_set_dumpable.2const.html
  cta::utils::setDumpableProcessAttribute(true);
  if (!cta::utils::getDumpableProcessAttribute()) {
    log(log::WARNING, "Failed to set the dumpable attribute. Core dumps may not be produced");
  }

  // Observe drive state while taped runs; the runtime has already initialized telemetry.
  telemetry::metrics::ScopedTapedStateMetrics stateMetrics;

  lc.log(log::INFO, "Initialising Catalogue");
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(config.catalogue.config_file);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;
  auto catalogueFactory =
    catalogue::CatalogueFactoryFactory::create(log, catalogueLogin, nbConns, nbArchiveFileListingConns);
  m_catalogue = catalogueFactory->create();
  lc.log(log::INFO, "Catalogue initialised successfully");

  m_schedulerContext = std::make_unique<SchedulerContext>(config, *m_catalogue, log);
  m_tapeDaemon = std::make_unique<TapeDaemon>(config, log, *m_catalogue, *m_schedulerContext);
  // Publish only after the daemon and all its dependencies have been constructed.
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

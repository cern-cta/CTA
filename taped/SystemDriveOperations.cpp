/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SystemDriveOperations.hpp"

#include "TapedConfig.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/Scheduler.hpp"
#include "session/CleanerSession.hpp"
#include "session/DataTransferSession.hpp"
#include "session/EmptyDriveProbe.hpp"
#include "system/Wrapper.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif

#include <algorithm>
#include <unistd.h>

namespace cta::tape::daemon {

namespace {

class SystemDriveOperations final : public DriveOperations {
public:
  SystemDriveOperations(const TapedConfig& config, log::Logger& log, const common::dataStructures::DriveInfo& driveInfo)
      : m_config(config),
        m_driveInfo(driveInfo),
        m_lc(log),
        m_mediaChanger(mediachanger::RmcProxy(config.rmcd.host,
                                              config.rmcd.port,
                                              config.rmcd.request_timeout_secs,
                                              config.rmcd.request_attempts),
                       log) {
    m_lc.log(log::INFO, "Initialising Catalogue");
    const rdbms::Login catalogueLogin = rdbms::Login::parseFile(m_config.catalogue.config_file);
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
    auto catalogueFactory =
      catalogue::CatalogueFactoryFactory::create(m_lc.logger(), catalogueLogin, nbConns, nbArchiveFileListingConns);
    m_catalogue = catalogueFactory->create();

    m_lc.log(log::INFO, "Catalogue initialised successfully");
    m_lc.log(log::INFO, "Initialising Scheduler");
#ifndef CTA_PGSCHED
    m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Taped",
                                                        utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                                        m_lc.logger());
#else
    m_schedDbInit = std::make_unique<SchedulerDBInit_t>("Taped",
                                                        utils::readSingleLineConfigFile(m_config.scheduler.config_file),
                                                        m_config.scheduler.number_of_connections,
                                                        m_lc.logger());
#endif
    m_schedDb = m_schedDbInit->getSchedDB(*m_catalogue, m_lc.logger());
    SchedulerDatabase::StatisticsCacheConfig statisticsCacheConfig;
    statisticsCacheConfig.tapeCacheMaxAgeSecs = m_config.scheduler.tape_cache_max_age_secs;
    statisticsCacheConfig.retrieveQueueCacheMaxAgeSecs = m_config.scheduler.retrieve_queue_cache_max_age_secs;
    m_schedDb->setStatisticsCacheConfig(statisticsCacheConfig);
    m_scheduler = std::make_unique<Scheduler>(*m_catalogue,
                                              *m_schedDb,
                                              m_config.scheduler.backend_name,
                                              m_config.mounts.minimum_queued_files,
                                              m_config.mounts.minimum_queued_bytes);

    m_lc.log(log::INFO, "Scheduler initialised successfully");
  }

  IScheduler& scheduler() override { return *m_scheduler; }

  bool logicalLibraryExists() override {
    const auto libraries = m_catalogue->LogicalLibrary()->getLogicalLibraries();
    return std::any_of(libraries.begin(), libraries.end(), [this](const auto& library) {
      return library.name == m_driveInfo.logicalLibrary;
    });
  }

  std::pair<bool, std::optional<std::string>> probeDrive() override {
    EmptyDriveProbe probe(m_lc.logger(), m_driveInfo, m_sysWrapper);
    const bool empty = probe.driveIsEmpty();
    return {empty, probe.getProbeErrorMsg()};
  }

  std::unique_ptr<TapeMount> getNextMount() override {
    // TODO: add timeout?
    if (m_scheduler->getNextMountDryRun(m_driveInfo.logicalLibrary, m_driveInfo.driveName, m_lc)) {
      return m_scheduler->getNextMount(m_driveInfo.logicalLibrary,
                                       m_driveInfo.driveName,
                                       m_lc,
                                       static_cast<uint64_t>(m_config.mounts.get_next_mount_timeout_secs) * 1000000);
    }
    return nullptr;
  }

  TransferSessionResult transfer(TapeMount& mount, TapeSessionTracker& tracker) override {
    DataTransferSession session(m_lc.logger(),
                                m_sysWrapper,
                                m_driveInfo,
                                m_mediaChanger,
                                mount,
                                tracker,
                                m_config.transfers,
                                m_config.mounts.tape_load_timeout_secs,
                                *m_scheduler);
    return session.execute();
  }

  bool clean(const std::optional<std::string>& vid, bool waitMediaInDrive, TapeSessionTracker& tracker) override {
    CleanerSession session(m_mediaChanger,
                           m_lc.logger(),
                           m_driveInfo,
                           m_sysWrapper,
                           vid.value_or(""),
                           waitMediaInDrive,
                           m_config.mounts.tape_load_timeout_secs,
                           *m_catalogue,
                           *m_scheduler,
                           &tracker);
    return session.execute() == DriveUsability::Reusable;
  }

  void sleep(unsigned int seconds) override { ::sleep(seconds); }

private:
  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo& m_driveInfo;
  log::LogContext m_lc;
  mediachanger::MediaChangerFacade m_mediaChanger;
  System::realWrapper m_sysWrapper;
  std::unique_ptr<catalogue::Catalogue> m_catalogue;
  std::unique_ptr<SchedulerDBInit_t> m_schedDbInit;
  std::unique_ptr<SchedulerDB_t> m_schedDb;
  std::unique_ptr<Scheduler> m_scheduler;
};

}  // namespace

std::unique_ptr<DriveOperations> makeSystemDriveOperations(const TapedConfig& config,
                                                           log::Logger& log,
                                                           const common::dataStructures::DriveInfo& driveInfo) {
  return std::make_unique<SystemDriveOperations>(config, log, driveInfo);
}

}  // namespace cta::tape::daemon

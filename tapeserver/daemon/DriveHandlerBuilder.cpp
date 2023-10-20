/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/log/LogContext.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#include "scheduler/Scheduler.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/CleanerSession.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "tapeserver/castor/tape/tapeserver/system/Wrapper.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/DriveHandlerBuilder.hpp"
#include "tapeserver/daemon/DriveHandlerProxy.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"

namespace cta {
namespace tape {
namespace daemon {

DriveHandlerBuilder::DriveHandlerBuilder(const TapedConfiguration* tapedConfig, const TpconfigLine* driveConfig,
  ProcessManager* pm) 
  : DriveHandler(*tapedConfig, *driveConfig, *pm) {
}

std::unique_ptr<cta::catalogue::Catalogue> DriveHandlerBuilder::createCatalogue(const std::string& processName) const {
  log::ScopedParamContainer params(m_processManager.logContext());
  params.add("fileCatalogConfigFile", m_tapedConfig.fileCatalogConfigFile.value());
  params.add("processName", processName);
  m_lc->log(log::DEBUG, "In DriveHandlerBuilder::createCatalogue(): "
    "will get catalogue login information.");
  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig.fileCatalogConfigFile.value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;
  m_lc->log(log::DEBUG, "In DriveHandlerBuilder::createCatalogue(): will connect to catalogue.");
  auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_lc->logger(),
  catalogueLogin, nbConns, nbArchiveFileListingConns);
  return catalogueFactory->create();
}

std::unique_ptr<cta::Scheduler> DriveHandlerBuilder::createScheduler(const std::string& prefixProcessName,
  const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount) {
  std::string processName;
  try {
    processName =  prefixProcessName + m_driveConfig.unitName;
    log::ScopedParamContainer params(*m_lc);
    params.add("processName", processName);
    m_lc->log(log::DEBUG, "In DriveHandlerBuilder::createScheduler(): will create agent entry. "
      "Enabling leaving non-empty agent behind.");
    m_sched_db_init = std::make_unique<SchedulerDBInit_t>(processName, m_tapedConfig.backendPath.value(),
      m_lc->logger(), true);
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(*m_lc);
    param.add("errorMessage", ex.getMessageValue());
    m_lc->log(log::CRIT, "In DriveHandlerBuilder::createScheduler(): failed to connect to objectstore or "
      "failed to instantiate agent entry. Reporting fatal error.");
    throw;
  }
  try {
    if (!m_catalogue) {
      m_catalogue = createCatalogue(processName);
    }
    m_sched_db = m_sched_db_init->getSchedDB(*m_catalogue, m_lc->logger());
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(*m_lc);
    param.add("errorMessage", ex.getMessageValue());
    m_lc->log(log::CRIT, "In DriveHandlerBuilder::createScheduler(): failed to instantiate catalogue. "
      "Reporting fatal error.");
    throw;
  }
  m_lc->log(log::DEBUG, "In DriveHandlerBuilder::createScheduler(): will create scheduler.");
  return std::make_unique<Scheduler>(*m_catalogue, *m_sched_db, minFilesToWarrantAMount, minBytesToWarrantAMount);
}

std::unique_ptr<castor::tape::tapeserver::daemon::CleanerSession> DriveHandlerBuilder::createCleanerSession(
  cta::Scheduler* scheduler) const {
  m_lc->log(log::DEBUG, "In DriveHandlerBuilder::createCleanerSession(): Creating cleaner session.");
  // Capabilities management.
  cta::server::ProcessCap capUtils;
  // Mounting management.
  cta::mediachanger::RmcProxy rmcProxy(
    m_tapedConfig.rmcPort.value(),
    m_tapedConfig.rmcNetTimeout.value(),
    m_tapedConfig.rmcRequestAttempts.value());
  cta::mediachanger::MediaChangerFacade mediaChangerFacade(rmcProxy, m_lc->logger());
  castor::tape::System::realWrapper sWrapper;
  return std::make_unique<castor::tape::tapeserver::daemon::CleanerSession>(
    capUtils,
    mediaChangerFacade,
    m_lc->logger(),
    m_driveConfig,
    sWrapper,
    m_sessionVid,
    true,
    m_tapedConfig.tapeLoadTimeout.value(),
    "",
    *m_catalogue,
    *scheduler);
}

std::unique_ptr<castor::tape::tapeserver::daemon::DataTransferSession> DriveHandlerBuilder::createDataTransferSession(
  cta::Scheduler* scheduler, cta::tape::daemon::DriveHandlerProxy* driveHandlerProxy, server::ProcessCap* capUtils,
  mediachanger::MediaChangerFacade* mediaChangerFacade, castor::tape::System::realWrapper* sWrapper) const {
  // Passing values from taped config to data transfer session config
  // When adding new config variables, be careful not to forget to pass them here
  castor::tape::tapeserver::daemon::DataTransferConfig dataTransferConfig;
  dataTransferConfig.bufsz = m_tapedConfig.bufferSizeBytes.value();
  dataTransferConfig.bulkRequestMigrationMaxBytes = m_tapedConfig.archiveFetchBytesFiles.value().maxBytes;
  dataTransferConfig.bulkRequestMigrationMaxFiles = m_tapedConfig.archiveFetchBytesFiles.value().maxFiles;
  dataTransferConfig.bulkRequestRecallMaxBytes = m_tapedConfig.retrieveFetchBytesFiles.value().maxBytes;
  dataTransferConfig.bulkRequestRecallMaxFiles = m_tapedConfig.retrieveFetchBytesFiles.value().maxFiles;
  dataTransferConfig.maxBytesBeforeFlush = m_tapedConfig.archiveFlushBytesFiles.value().maxBytes;
  dataTransferConfig.maxFilesBeforeFlush = m_tapedConfig.archiveFlushBytesFiles.value().maxFiles;
  dataTransferConfig.nbBufs = m_tapedConfig.bufferCount.value();
  dataTransferConfig.nbDiskThreads = m_tapedConfig.nbDiskThreads.value();
  dataTransferConfig.useLbp = true;
  dataTransferConfig.useRAO = (m_tapedConfig.useRAO.value() == "yes");
  dataTransferConfig.raoLtoAlgorithm = m_tapedConfig.raoLtoAlgorithm.value();
  dataTransferConfig.raoLtoAlgorithmOptions = m_tapedConfig.raoLtoOptions.value();
  dataTransferConfig.externalFreeDiskSpaceScript = m_tapedConfig.externalFreeDiskSpaceScript.value();
  dataTransferConfig.tapeLoadTimeout = m_tapedConfig.tapeLoadTimeout.value();
  dataTransferConfig.xrootTimeout = 0;
  dataTransferConfig.useEncryption = (m_tapedConfig.useEncryption.value() == "yes");
  dataTransferConfig.externalEncryptionKeyScript = m_tapedConfig.externalEncryptionKeyScript.value();
  dataTransferConfig.wdIdleSessionTimer = m_tapedConfig.wdIdleSessionTimer.value();
  dataTransferConfig.wdGlobalLockAcqMaxSecs = m_tapedConfig.wdGlobalLockAcqMaxSecs.value();
  dataTransferConfig.wdNoBlockMoveMaxSecs = m_tapedConfig.wdNoBlockMoveMaxSecs.value();

  return std::make_unique<castor::tape::tapeserver::daemon::DataTransferSession>(
    cta::utils::getShortHostname(),
    m_lc->logger(),
    *sWrapper,
    m_driveConfig,
    *mediaChangerFacade,
    *driveHandlerProxy,
    *capUtils,
    dataTransferConfig,
    *scheduler);
}

} // namespace daemon
} // namespace tape
} // namespace cta

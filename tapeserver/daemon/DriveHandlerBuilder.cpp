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
#include "rdbms/Login.hpp"
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#include "scheduler/Scheduler.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/DriveHandlerBuilder.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"

namespace cta {
namespace tape {
namespace daemon {

DriveHandlerBuilder::DriveHandlerBuilder(const TapedConfiguration* tapedConfig, const TpconfigLine* driveConfig,
  ProcessManager* pm) 
  : DriveHandler(*tapedConfig, *driveConfig, *pm), m_tapedConfig(tapedConfig), m_driveConfig(driveConfig),
    m_processManager(pm) {
}

void DriveHandlerBuilder::build() {
  // auto dh = std::make_unique<DriveHandler>(*m_tapedConfig, *m_driveConfig, *m_processManager);
  m_catalogue = std::shared_ptr<cta::catalogue::Catalogue>(createCatalogue());
  m_scheduler = std::shared_ptr<Scheduler>(createScheduler(m_catalogue));
  // setScheduler(scheduler);
  // setCatalogue(catalogue);
  // return std::unique_ptr<DriveHandler>(this);
}

std::unique_ptr<catalogue::Catalogue> DriveHandlerBuilder::createCatalogue() {
  log::ScopedParamContainer params(m_processManager->logContext());
  params.add("fileCatalogConfigFile", m_tapedConfig->fileCatalogConfigFile.value());
  m_processManager->logContext().log(log::DEBUG, "In DriveHandlerBuilder::createCatalogue(): "
    "will get catalogue login information.");
  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig->fileCatalogConfigFile.value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;
  m_processManager->logContext().log(log::DEBUG, "In DriveHandlerBuilder::createCatalogue(): will connect to catalogue.");
  auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_processManager->logContext().logger(),
  catalogueLogin, nbConns, nbArchiveFileListingConns);
  return catalogueFactory->create();
}

std::unique_ptr<Scheduler> DriveHandlerBuilder::createScheduler(std::shared_ptr<cta::catalogue::Catalogue> catalogue) {
  auto& lc = m_processManager->logContext();
  // std::unique_ptr<SchedulerDBInit_t> sched_db_init;
  try {
    std::string processName = "DriveProcess-";
    processName += m_driveConfig->unitName;
    log::ScopedParamContainer params(lc);
    params.add("processName", processName);
    lc.log(log::DEBUG, "In DriveHandlerBuilder::createScheduler(): will create agent entry. Enabling leaving non-empty agent behind.");
    m_sched_db_init.reset(new SchedulerDBInit_t(processName, m_tapedConfig->backendPath.value(), lc.logger(), true));
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandlerBuilder::createScheduler(): failed to connect to objectstore or failed to instantiate agent entry. Reporting fatal error.");
    // driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    // sleep(1);
    throw;
  }
  // std::shared_ptr<SchedulerDB_t> sched_db;
  try {
    m_sched_db = m_sched_db_init->getSchedDB(*catalogue, lc.logger());
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer param(lc);
    param.add("errorMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In DriveHandlerBuilder::createScheduler(): failed to instantiate catalogue. Reporting fatal error.");
    // driveHandlerProxy.reportState(tape::session::SessionState::Fatal, tape::session::SessionType::Undetermined, "");
    // sleep(1);
    throw;
  }
  lc.log(log::DEBUG, "In DriveHandlerBuilder::createScheduler(): will create scheduler.");
  return std::make_unique<Scheduler>(*catalogue, *m_sched_db, m_tapedConfig->mountCriteria.value().maxFiles,
    m_tapedConfig->mountCriteria.value().maxBytes);
}

} // namespace daemon
} // namespace tape
} // namespace cta

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
#include "tapeserver/daemon/DriveHandlerBuilder.hpp"
#include "tapeserver/daemon/DriveHandler.hpp"
#include "tapeserver/daemon/ProcessManager.hpp"
#include "tapeserver/daemon/TapedConfiguration.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"

namespace cta {
namespace tape {
namespace daemon {

DriveHandlerBuilder::DriveHandlerBuilder(const TapedConfiguration* tapedConfig, const TpconfigLine* driveConfig,
  ProcessManager* pm)
  : m_tapedConfig(tapedConfig), m_driveConfig(driveConfig), m_processManager(pm) {
  
}

std::unique_ptr<DriveHandler> DriveHandlerBuilder::build() {
  auto dh = std::make_unique<DriveHandler>(*m_tapedConfig, *m_driveConfig, *m_processManager);
  dh->setCatalogue(createCatalogue());
  return dh;
}

std::unique_ptr<cta::catalogue::Catalogue> DriveHandlerBuilder::createCatalogue() {
  log::ScopedParamContainer params(m_processManager->logContext());
  params.add("fileCatalogConfigFile", m_tapedConfig->fileCatalogConfigFile.value());
  m_processManager->logContext().log(log::DEBUG, "In DriveHandlerBuilder::createCatalogue(): "
    "will get catalogue login information.");
  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(m_tapedConfig->fileCatalogConfigFile.value());
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;
  m_processManager->logContext().log(log::DEBUG, "In DriveHandler::createCatalogue(): will connect to catalogue.");
  auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_processManager->logContext().logger(),
  catalogueLogin, nbConns, nbArchiveFileListingConns);
  return catalogueFactory->create();
}

// std::unique_ptr<Scheduler> DriveHandlerBuilder::createScheduler() {

// }


} // namespace daemon
} // namespace tape
} // namespace cta

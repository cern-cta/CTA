/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <memory>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "catalogue/retrywrappers/TapeFileCatalogueRetryWrapper.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"

namespace cta::catalogue {

TapeFileCatalogueRetryWrapper::TapeFileCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {
}

void TapeFileCatalogueRetryWrapper::filesWrittenToTape(const std::set<TapeItemWrittenPointer> &event) {
  return retryOnLostConnection(m_log, [this,&event] {
    return m_catalogue->TapeFile()->filesWrittenToTape(event);
  }, m_maxTriesToConnect);
}

void TapeFileCatalogueRetryWrapper::deleteTapeFileCopy(common::dataStructures::ArchiveFile &file,
  const std::string &reason) {
  return retryOnLostConnection(m_log, [this,&file,&reason] {
    return m_catalogue->TapeFile()->deleteTapeFileCopy(file, reason);
  }, m_maxTriesToConnect);
}

common::dataStructures::RetrieveFileQueueCriteria TapeFileCatalogueRetryWrapper::prepareToRetrieveFile(
  const std::string &diskInstanceName, const uint64_t archiveFileId,
  const common::dataStructures::RequesterIdentity &user, const std::optional<std::string> & activity,
  log::LogContext &lc, const std::optional<std::string> &mountPolicyName) {
  return retryOnLostConnection(m_log, [this,&diskInstanceName,&archiveFileId,&user,&activity,&lc,&mountPolicyName] {
    return m_catalogue->TapeFile()->prepareToRetrieveFile(diskInstanceName, archiveFileId, user, activity, lc, mountPolicyName);
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue

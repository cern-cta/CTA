/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/retrywrappers/TapeFileCatalogueRetryWrapper.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/RetrieveFileQueueCriteria.hpp"

#include <memory>
#include <string>

namespace cta::catalogue {

TapeFileCatalogueRetryWrapper::TapeFileCatalogueRetryWrapper(Catalogue& catalogue,
                                                             log::Logger& log,
                                                             const uint32_t maxTriesToConnect)
    : m_catalogue(catalogue),
      m_log(log),
      m_maxTriesToConnect(maxTriesToConnect) {}

void TapeFileCatalogueRetryWrapper::filesWrittenToTape(const std::set<TapeItemWrittenPointer>& event) {
  return retryOnLostConnection(
    m_log,
    [this, &event] { return m_catalogue.TapeFile()->filesWrittenToTape(event); },
    m_maxTriesToConnect);
}

void TapeFileCatalogueRetryWrapper::deleteTapeFileCopy(common::dataStructures::ArchiveFile& file,
                                                       const std::string& reason) {
  return retryOnLostConnection(
    m_log,
    [this, &file, &reason] { return m_catalogue.TapeFile()->deleteTapeFileCopy(file, reason); },
    m_maxTriesToConnect);
}

common::dataStructures::RetrieveFileQueueCriteria
TapeFileCatalogueRetryWrapper::prepareToRetrieveFile(const std::string& diskInstanceName,
                                                     const uint64_t archiveFileId,
                                                     const common::dataStructures::RequesterIdentity& user,
                                                     const std::optional<std::string>& activity,
                                                     log::LogContext& lc,
                                                     const std::optional<std::string>& mountPolicyName) {
  return retryOnLostConnection(
    m_log,
    [this, &diskInstanceName, &archiveFileId, &user, &activity, &lc, &mountPolicyName] {
      return m_catalogue.TapeFile()
        ->prepareToRetrieveFile(diskInstanceName, archiveFileId, user, activity, lc, mountPolicyName);
    },
    m_maxTriesToConnect);
}

}  // namespace cta::catalogue

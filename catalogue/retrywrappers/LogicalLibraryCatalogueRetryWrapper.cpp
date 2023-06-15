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

#include "catalogue/Catalogue.hpp"
#include "catalogue/retrywrappers/LogicalLibraryCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"

namespace cta {
namespace catalogue {

LogicalLibraryCatalogueRetryWrapper::LogicalLibraryCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                                                         log::Logger& log,
                                                                         const uint32_t maxTriesToConnect) :
m_catalogue(catalogue),
m_log(log),
m_maxTriesToConnect(maxTriesToConnect) {}

void LogicalLibraryCatalogueRetryWrapper::createLogicalLibrary(const common::dataStructures::SecurityIdentity& admin,
                                                               const std::string& name,
                                                               const bool isDisabled,
                                                               const std::string& comment) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->LogicalLibrary()->createLogicalLibrary(admin, name, isDisabled, comment); },
    m_maxTriesToConnect);
}

void LogicalLibraryCatalogueRetryWrapper::deleteLogicalLibrary(const std::string& name) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->LogicalLibrary()->deleteLogicalLibrary(name); }, m_maxTriesToConnect);
}

std::list<common::dataStructures::LogicalLibrary> LogicalLibraryCatalogueRetryWrapper::getLogicalLibraries() const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->LogicalLibrary()->getLogicalLibraries(); }, m_maxTriesToConnect);
}

void LogicalLibraryCatalogueRetryWrapper::modifyLogicalLibraryName(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& currentName,
  const std::string& newName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->LogicalLibrary()->modifyLogicalLibraryName(admin, currentName, newName); },
    m_maxTriesToConnect);
}

void LogicalLibraryCatalogueRetryWrapper::modifyLogicalLibraryComment(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& comment) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->LogicalLibrary()->modifyLogicalLibraryComment(admin, name, comment); },
    m_maxTriesToConnect);
}

void LogicalLibraryCatalogueRetryWrapper::modifyLogicalLibraryDisabledReason(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const std::string& disabledReason) {
  return retryOnLostConnection(
    m_log,
    [&] { return m_catalogue->LogicalLibrary()->modifyLogicalLibraryDisabledReason(admin, name, disabledReason); },
    m_maxTriesToConnect);
}

void LogicalLibraryCatalogueRetryWrapper::setLogicalLibraryDisabled(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const bool disabledValue) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->LogicalLibrary()->setLogicalLibraryDisabled(admin, name, disabledValue); },
    m_maxTriesToConnect);
}

}  // namespace catalogue
}  // namespace cta
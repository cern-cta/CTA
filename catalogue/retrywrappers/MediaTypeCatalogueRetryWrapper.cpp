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

#include <list>
#include <memory>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"
#include "catalogue/retrywrappers/MediaTypeCatalogueRetryWrapper.hpp"
#include "catalogue/retrywrappers/retryOnLostConnection.hpp"
#include "common/log/Logger.hpp"

namespace cta {
namespace catalogue {

MediaTypeCatalogueRetryWrapper::MediaTypeCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
                                                               log::Logger& log,
                                                               const uint32_t maxTriesToConnect) :
m_catalogue(catalogue),
m_log(log),
m_maxTriesToConnect(maxTriesToConnect) {}

void MediaTypeCatalogueRetryWrapper::createMediaType(const common::dataStructures::SecurityIdentity& admin,
                                                     const MediaType& mediaType) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->createMediaType(admin, mediaType); }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::deleteMediaType(const std::string& name) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->deleteMediaType(name); }, m_maxTriesToConnect);
}

std::list<MediaTypeWithLogs> MediaTypeCatalogueRetryWrapper::getMediaTypes() const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->getMediaTypes(); }, m_maxTriesToConnect);
}

MediaType MediaTypeCatalogueRetryWrapper::getMediaTypeByVid(const std::string& vid) const {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->getMediaTypeByVid(vid); }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeName(const common::dataStructures::SecurityIdentity& admin,
                                                         const std::string& currentName,
                                                         const std::string& newName) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeName(admin, currentName, newName); },
    m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity& admin,
                                                              const std::string& name,
                                                              const std::string& cartridge) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeCartridge(admin, name, cartridge); },
    m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeCapacityInBytes(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint64_t capacityInBytes) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeCapacityInBytes(admin, name, capacityInBytes); },
    m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypePrimaryDensityCode(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint8_t primaryDensityCode) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypePrimaryDensityCode(admin, name, primaryDensityCode); },
    m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeSecondaryDensityCode(
  const common::dataStructures::SecurityIdentity& admin,
  const std::string& name,
  const uint8_t secondaryDensityCode) {
  return retryOnLostConnection(
    m_log,
    [&] { return m_catalogue->MediaType()->modifyMediaTypeSecondaryDensityCode(admin, name, secondaryDensityCode); },
    m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& name,
                                                            const std::optional<std::uint32_t>& nbWraps) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeNbWraps(admin, name, nbWraps); }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& name,
                                                            const std::optional<std::uint64_t>& minLPos) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeMinLPos(admin, name, minLPos); }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& name,
                                                            const std::optional<std::uint64_t>& maxLPos) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeMaxLPos(admin, name, maxLPos); }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity& admin,
                                                            const std::string& name,
                                                            const std::string& comment) {
  return retryOnLostConnection(
    m_log, [&] { return m_catalogue->MediaType()->modifyMediaTypeComment(admin, name, comment); }, m_maxTriesToConnect);
}
}  // namespace catalogue
}  // namespace cta
/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

namespace cta::catalogue {

MediaTypeCatalogueRetryWrapper::MediaTypeCatalogueRetryWrapper(const std::unique_ptr<Catalogue>& catalogue,
  log::Logger &log, const uint32_t maxTriesToConnect)
  : m_catalogue(catalogue), m_log(log), m_maxTriesToConnect(maxTriesToConnect) {}

void MediaTypeCatalogueRetryWrapper::createMediaType(const common::dataStructures::SecurityIdentity &admin,
  const MediaType &mediaType) {
  return retryOnLostConnection(m_log, [this,&admin,&mediaType] {
    return m_catalogue->MediaType()->createMediaType(admin, mediaType);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::deleteMediaType(const std::string &name) {
  return retryOnLostConnection(m_log, [this,&name] {
    return m_catalogue->MediaType()->deleteMediaType(name);
  }, m_maxTriesToConnect);
}

std::list<MediaTypeWithLogs> MediaTypeCatalogueRetryWrapper::getMediaTypes() const {
  return retryOnLostConnection(m_log, [this] {
    return m_catalogue->MediaType()->getMediaTypes();
  }, m_maxTriesToConnect);
}

MediaType MediaTypeCatalogueRetryWrapper::getMediaTypeByVid(const std::string & vid) const {
  return retryOnLostConnection(m_log, [this,&vid] {
    return m_catalogue->MediaType()->getMediaTypeByVid(vid);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeName(const common::dataStructures::SecurityIdentity &admin,
  const std::string &currentName, const std::string &newName) {
  return retryOnLostConnection(m_log, [this,&admin,&currentName,&newName] {
    return m_catalogue->MediaType()->modifyMediaTypeName(admin, currentName, newName);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeCartridge(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &cartridge) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&cartridge] {
    return m_catalogue->MediaType()->modifyMediaTypeCartridge(admin, name, cartridge);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeCapacityInBytes(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t capacityInBytes) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&capacityInBytes] {
    return m_catalogue->MediaType()->modifyMediaTypeCapacityInBytes(admin, name, capacityInBytes);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypePrimaryDensityCode(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t primaryDensityCode) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&primaryDensityCode] {
    return m_catalogue->MediaType()->modifyMediaTypePrimaryDensityCode(admin, name, primaryDensityCode);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeSecondaryDensityCode(
  const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint8_t secondaryDensityCode) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&secondaryDensityCode] {
    return m_catalogue->MediaType()->modifyMediaTypeSecondaryDensityCode(admin, name, secondaryDensityCode);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeNbWraps(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint32_t> &nbWraps) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&nbWraps] {
    return m_catalogue->MediaType()->modifyMediaTypeNbWraps(admin, name, nbWraps);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeMinLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &minLPos) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&minLPos] {
    return m_catalogue->MediaType()->modifyMediaTypeMinLPos(admin, name, minLPos);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeMaxLPos(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::optional<std::uint64_t> &maxLPos) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&maxLPos] {
    return m_catalogue->MediaType()->modifyMediaTypeMaxLPos(admin, name, maxLPos);
  }, m_maxTriesToConnect);
}

void MediaTypeCatalogueRetryWrapper::modifyMediaTypeComment(const common::dataStructures::SecurityIdentity &admin,
  const std::string &name, const std::string &comment) {
  return retryOnLostConnection(m_log, [this,&admin,&name,&comment] {
    return m_catalogue->MediaType()->modifyMediaTypeComment(admin, name, comment);
  }, m_maxTriesToConnect);
}

} // namespace cta::catalogue

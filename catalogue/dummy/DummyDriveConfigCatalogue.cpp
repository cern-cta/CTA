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

#include "catalogue/dummy/DummyDriveConfigCatalogue.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

void DummyDriveConfigCatalogue::createTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> DummyDriveConfigCatalogue::getTapeDriveConfigs() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

std::list<std::pair<std::string, std::string>> DummyDriveConfigCatalogue::getTapeDriveConfigNamesAndKeys() const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyDriveConfigCatalogue::modifyTapeDriveConfig(const std::string &tapeDriveName, const std::string &category,
  const std::string &keyName, const std::string &value, const std::string &source) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}
std::optional<std::tuple<std::string, std::string, std::string>> DummyDriveConfigCatalogue::getTapeDriveConfig(
  const std::string &tapeDriveName, const std::string &keyName) const {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

void DummyDriveConfigCatalogue::deleteTapeDriveConfig(const std::string &tapeDriveName, const std::string &keyName) {
  throw exception::Exception(std::string("In ") + __PRETTY_FUNCTION__ + ": not implemented");
}

} // namespace cta::catalogue

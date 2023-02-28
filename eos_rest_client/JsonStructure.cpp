/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include <iostream>
#include <fstream>
#include <string>

#include "eos_rest_client/JsonStructure.hpp"
#include "common/exception/UserError.hpp"

namespace cta::rest {

  JsonStructure::JsonStructure(const std::string& jsonString) : m_jsonString{jsonString} {}

  std::unordered_map<std::string, std::any> JsonStructure::getFileInfo() const {
    JsonStructureLayer fileInfoLayer(EOS_REST_ENDPOINT::FILE_INFO, m_jsonString);
    const std::string locationsJsonString = std::any_cast<std::string>(fileInfoLayer.readJson().at("locations"));
    const std::string xAttrJsonString = std::any_cast<std::string>(fileInfoLayer.readJson().at("xattr"));
    JsonStructureLayer locations(EOS_REST_ENDPOINT::LOCATIONS, locationsJsonString);
    JsonStructureLayer xAttr(EOS_REST_ENDPOINT::LOCATIONS, locationsJsonString);

    std::unordered_map<std::string, std::any> fileInfo = fileInfoLayer.readJson();
    fileInfo["locations"] = locations.readJson();
    fileInfo["xattr"] = xAttr.readJson();

    return fileInfo;
  }
} // namespace cta::rest
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

#include <filesystem>
#include <iostream>
#include <fstream>

#include "cmdline/standalone_cli_tools/eos_namespace_injection/MetaData.hpp"
#include "common/exception/UserError.hpp"

namespace cta::cliTool {

MetaData::MetaData(const std::filesystem::path& jsonPath) {
  readJson(jsonPath);
}

void MetaData::readJson(const std::filesystem::path& jsonPath) {
  std::ifstream infile(jsonPath);
  if (infile.fail()) {
    throw exception::UserError("Could not open " + jsonPath.generic_string());
  }

  std::string line;
  while (std::getline(infile, line)) {
    if (!line.empty()) {
      cta::utils::json::object::JSONCObject jsonObject;
      buildFromJSON(line);

      MetaDataObject metaDataObject;
      metaDataObject.eosPath = jsonGetValue<std::string>("eosPath");
      metaDataObject.diskInstance = jsonGetValue<std::string>("diskInstance");
      metaDataObject.archiveId = jsonGetValue<std::string>("archiveId");
      metaDataObject.size = jsonGetValue<std::string>("size");
      metaDataObject.checksumType = jsonGetValue<std::string>("checksumType");
      metaDataObject.checksumValue = jsonGetValue<std::string>("checksumValue");

      m_mdCollection.push_back(metaDataObject);
    }
  }
  infile.close();
}

}  // namespace cta::cliTool
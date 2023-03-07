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

#include "cmdline/standalone_cli_tools/change_storage_class/JsonFileData.hpp"
#include "common/exception/UserError.hpp"

namespace cta::cliTool {

JsonFileData::JsonFileData(const std::filesystem::path &jsonPath) {
  readJson(jsonPath);
}

void JsonFileData::readJson(const std::filesystem::path &jsonPath) {
  std::ifstream infile(jsonPath);
  if(infile.fail()) {
    throw exception::UserError("Could not open " + jsonPath.generic_string());
  }

  std::string line;
  while (std::getline(infile, line)) {
    if(!line.empty()) {
      cta::utils::json::object::JSONCObject jsonObject;
      buildFromJSON(line);

      JsonFileDataObject JsonFileDataObject;
      JsonFileDataObject.archiveId = jsonGetValue<std::string>("archiveId");
      JsonFileDataObject.fid       = std::to_string(jsonGetValue<uint64_t>("fid"));
      JsonFileDataObject.instance  = jsonGetValue<std::string>("instance");

      m_jsonArgumentsCollection.push_back(JsonFileDataObject);
    }
  }
  infile.close();
}

} // namespace cta::cliTool
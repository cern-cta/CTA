/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
      JsonFileDataObject.archiveFileId     = jsonGetValue<std::string>("archiveFileId");
      JsonFileDataObject.fid           = std::to_string(jsonGetValue<uint64_t>("fid"));
      JsonFileDataObject.instance      = jsonGetValue<std::string>("instance");
      JsonFileDataObject.storageClass  = jsonGetValue<std::string>("storageclass");

      m_jsonArgumentsCollection.push_back(JsonFileDataObject);
    }
  }
  infile.close();
}

} // namespace cta::cliTool

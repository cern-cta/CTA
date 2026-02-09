/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tools/standalone_cli_tools/eos_namespace_injection/MetaData.hpp"

#include "common/exception/UserError.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>

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

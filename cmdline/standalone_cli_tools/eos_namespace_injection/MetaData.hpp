/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <filesystem>
#include <string>
#include <list>

#include "common/json/object/JSONCObject.hpp"

namespace cta::cliTool {

struct MetaDataObject {
  std::string eosPath;
  std::string diskInstance;
  std::string diskFileId;
  std::string creationTime;
  std::string archiveId;
  std::string size;
  std::string checksumType;
  std::string checksumValue;
  std::string storageClass;
};

class MetaData : public cta::utils::json::object::JSONCObject{
  public:
    /**
     * Constructor
     */
    explicit MetaData(const std::filesystem::path& jsonPath);

    /**
     * Reads the provided JSON file
     */
    std::list<MetaDataObject> m_mdCollection;
  private:
    /**
     * Reads the provided JSON file
     */
    void readJson(const std::filesystem::path& path);
  };

} // namespace cta::cliTool

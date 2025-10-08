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

struct JsonFileDataObject {
  std::string archiveFileId;
  std::string fid;
  std::string instance;
  std::string storageClass;
};

class JsonFileData : public cta::utils::json::object::JSONCObject{
  public:
    /**
     * Constructor
     */
    explicit JsonFileData(const std::filesystem::path& jsonPath);

    /**
     * List of argument objects
     */
    std::list<JsonFileDataObject> m_jsonArgumentsCollection;
  private:
    /**
     * Reads the provided json file
     */
    void readJson(const std::filesystem::path& path);
  };

} // namespace cta::cliTool

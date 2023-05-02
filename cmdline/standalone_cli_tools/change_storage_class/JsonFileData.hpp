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
    JsonFileData(const std::filesystem::path &jsonPath);

    /**
    * List of argument objects
    */
    std::list<JsonFileDataObject> m_jsonArgumentsCollection;
  private:
    /**
    * Reads the provided json file
    */
    void readJson(const std::filesystem::path &path);
 };
 } // namespace cta::cliTool
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

struct MetaDataObject {
  std::string eosPath;
  std::string diskInstance;
  std::string fxId;
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
    MetaData(const std::filesystem::path &jsonPath);

    /**
    * Reads the provided json file
    */
    std::list<MetaDataObject> m_mdCollection;
  private:
    /**
    * Reads the provided json file
    */
    void readJson(const std::filesystem::path &path);
 };
 } // namespace cta::cliTool
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

#include <any>
#include <list>
#include <unordered_map>
#include <string>

#include "common/json/object/JSONCObject.hpp"

namespace cta::rest {

/**
* Enum values for each EOS REST endpoint
*/
enum class EOS_REST_ENDPOINT {
  FILE_INFO,
  LOCATIONS,
  XATTR,
};

class JsonStructureLayer : public cta::utils::json::object::JSONCObject{
  public:
    /**
    * Constructor
    */
    explicit JsonStructureLayer(const EOS_REST_ENDPOINT &eosRestEndpoint, const std::string& jsonString);

    /**
    * Reads the provided json string, reads the first layer
    */
    std::unordered_map<std::string, std::any> readJson();
  private:

    std::unordered_map<std::string, std::any> getFileInfo();

    std::unordered_map<std::string, std::any> getLocations();

    std::unordered_map<std::string, std::any> getXAttr();

    std::unordered_map<std::string, std::any> populate(const std::list<std::string>& keys);

    /**
    * Enum describing which endpoint is being parsed to json
    */
    EOS_REST_ENDPOINT m_eosRestEndpoint;

    std::string m_jsonString;
 };
 } // namespace cta::rest
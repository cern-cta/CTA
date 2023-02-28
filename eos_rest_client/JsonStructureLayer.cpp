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
#include <unordered_map>

#include "eos_rest_client/JsonStructureLayer.hpp"

namespace cta::rest {

JsonStructureLayer::JsonStructureLayer(const EOS_REST_ENDPOINT &eosRestEndpoint, const std::string& jsonString) : m_eosRestEndpoint{eosRestEndpoint}, m_jsonString{jsonString} {}

std::unordered_map<std::string, std::any> JsonStructureLayer::readJson() {
  cta::utils::json::object::JSONCObject jsonObject;
  buildFromJSON(m_jsonString);

  switch (m_eosRestEndpoint) {
  case EOS_REST_ENDPOINT::FILE_INFO:
    return getFileInfo();
  case EOS_REST_ENDPOINT::LOCATIONS:
    return getLocations();
  case EOS_REST_ENDPOINT::XATTR:
    return getXAttr();
  }
  return {};
}

std::unordered_map<std::string, std::any> JsonStructureLayer::getFileInfo() {

  std::list<std::string> keys = {
    "atime",
    "atime_ns",
    "btime",
    "btime_ns",
    "checksumtype",
    "checksumvalue",
    "ctime",
    "ctime_ns",
    "detached",
    "etag",
    "fxid",
    "gid",
    "id",
    "inode",
    "layout",
    "locations",
    "mode",
    "mtime",
    "mtime_ns",
    "name",
    "nlink",
    "nstripes",
    "path",
    "pid",
    "size",
    "uid",
    "xattr"
  };

  std::unordered_map<std::string, std::any> fileInfo = populate(keys);
  return fileInfo;
}

std::unordered_map<std::string, std::any> JsonStructureLayer::getLocations() {

  std::list<std::string> keys = {
    "fsid",
    "fstpath",
    "geotag",
    "host",
    "mountpoint",
    "schedgroup",
    "status"
  };

  std::unordered_map<std::string, std::any> locations = populate(keys);
  return locations;
}

std::unordered_map<std::string, std::any> JsonStructureLayer::getXAttr() {

  std::list<std::string> keys = {
    "sys.archive.error",
		"sys.archive.file_id",
		"sys.archive.storage_class",
		"sys.cta.archive.objectstore.id",
		"sys.eos.btime",
		"sys.fs.tracking",
		"sys.utrace",
		"sys.vtrace"
  };

  std::unordered_map<std::string, std::any> locations = populate(keys);
  return locations;
}

std::unordered_map<std::string, std::any> JsonStructureLayer::populate(const std::list<std::string>& keys) {
  std::unordered_map<std::string, std::any> jsonMap;
  for(const auto& key : keys) {
    auto jsonValue = jsonGetValue<std::any>(key);
    if (jsonValue.has_value()) {
      jsonMap[key] = jsonValue;
    } else {
      
    }
  }
  return jsonMap;
}

} // namespace cta::rest
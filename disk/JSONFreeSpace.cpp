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

#include "JSONFreeSpace.hpp"

namespace cta {
namespace disk {

JSONFreeSpace::JSONFreeSpace() : JSONCObject() {}

void JSONFreeSpace::buildFromJSON(const std::string& json) {
  JSONCObject::buildFromJSON(json);
  m_freeSpace = jsonGetValue<uint64_t>("freeSpace");
}

std::string JSONFreeSpace::getJSON() {
  reinitializeJSONCObject();
  jsonSetValue("freeSpace", m_freeSpace);
  return JSONCObject::getJSON();
}

std::string JSONFreeSpace::getExpectedJSONToBuildObject() const {
  return "{\"freeSpace\":42}";
}

JSONFreeSpace::~JSONFreeSpace() {}

}  // namespace disk
}  // namespace cta
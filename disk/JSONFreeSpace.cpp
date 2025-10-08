/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JSONFreeSpace.hpp"

namespace cta::disk {

JSONFreeSpace::JSONFreeSpace(): JSONCObject() {
}

void JSONFreeSpace::buildFromJSON(const std::string& json) {
  JSONCObject::buildFromJSON(json);
  m_freeSpace = jsonGetValue<uint64_t>("freeSpace");
}

std::string JSONFreeSpace::getJSON(){
  reinitializeJSONCObject();
  jsonSetValue("freeSpace",m_freeSpace);
  return JSONCObject::getJSON();
}

std::string JSONFreeSpace::getExpectedJSONToBuildObject() const {
  return "{\"freeSpace\":42}";
}

} // namespace cta::disk

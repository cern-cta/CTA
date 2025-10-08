/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JSONDiskSystem.hpp"

namespace cta::disk {

JSONDiskSystem::JSONDiskSystem() : JSONCObject() {
  diskInstanceSpace.refreshInterval = 0;
  targetedFreeSpace = 0;
  sleepTime = 0;
}

JSONDiskSystem::JSONDiskSystem(const DiskSystem& diskSystem):JSONCObject(){
  if(this != &diskSystem){
    name = diskSystem.name;
    fileRegexp = diskSystem.fileRegexp;
    diskInstanceSpace.freeSpaceQueryURL = diskSystem.diskInstanceSpace.freeSpaceQueryURL;
    diskInstanceSpace.refreshInterval = diskSystem.diskInstanceSpace.refreshInterval;
    targetedFreeSpace = diskSystem.targetedFreeSpace;
    sleepTime = diskSystem.sleepTime;
  }
}

void JSONDiskSystem::buildFromJSON(const std::string& json) {
  JSONCObject::buildFromJSON(json);
  name = jsonGetValue<std::string>("name");
  fileRegexp = jsonGetValue<std::string>("fileRegexp");
  diskInstanceSpace.freeSpaceQueryURL = jsonGetValue<std::string>("freeSpaceQueryURL");
  diskInstanceSpace.refreshInterval = jsonGetValue<time_t>("refreshInterval");
  targetedFreeSpace = jsonGetValue<uint64_t>("targetedFreeSpace");
  sleepTime = jsonGetValue<time_t>("sleepTime");
}

std::string JSONDiskSystem::getJSON() {
  reinitializeJSONCObject();
  jsonSetValue("name",name);
  jsonSetValue("fileRegexp",fileRegexp);
  jsonSetValue("freeSpaceQueryURL",diskInstanceSpace.freeSpaceQueryURL);
  jsonSetValue("refreshInterval", diskInstanceSpace.refreshInterval);
  jsonSetValue("targetedFreeSpace",targetedFreeSpace);
  jsonSetValue("sleepTime",sleepTime);
  return JSONCObject::getJSON();
}

} // namespace cta::disk

/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "JSONDiskSystem.hpp"

namespace cta { namespace disk {
  
JSONDiskSystem::JSONDiskSystem() : JSONCObject() {
  refreshInterval = 0;
  targetedFreeSpace = 0;
  sleepTime = 0;
}

JSONDiskSystem::JSONDiskSystem(const DiskSystem& diskSystem):JSONCObject(){
  if(this != &diskSystem){
    name = diskSystem.name;
    fileRegexp = diskSystem.fileRegexp;
    freeSpaceQueryURL = diskSystem.freeSpaceQueryURL;
    refreshInterval = diskSystem.refreshInterval;
    targetedFreeSpace = diskSystem.targetedFreeSpace;
    sleepTime = diskSystem.sleepTime;
  }
}

void JSONDiskSystem::buildFromJSON(const std::string& json) {
  JSONCObject::buildFromJSON(json);
  name = jsonGetValue<std::string>("name");
  fileRegexp = jsonGetValue<std::string>("fileRegexp");
  freeSpaceQueryURL = jsonGetValue<std::string>("freeSpaceQueryURL");
  refreshInterval = jsonGetValue<time_t>("refreshInterval");
  targetedFreeSpace = jsonGetValue<uint64_t>("targetedFreeSpace");
  sleepTime = jsonGetValue<time_t>("sleepTime");
}

std::string JSONDiskSystem::getJSON() {
  reinitializeJSONCObject();
  jsonSetValue("name",name);
  jsonSetValue("fileRegexp",fileRegexp);
  jsonSetValue("freeSpaceQueryURL",freeSpaceQueryURL);
  jsonSetValue("refreshInterval",refreshInterval);
  jsonSetValue("targetedFreeSpace",targetedFreeSpace);
  jsonSetValue("sleepTime",sleepTime);
  return JSONCObject::getJSON();
}

JSONDiskSystem::~JSONDiskSystem() {
}

}}

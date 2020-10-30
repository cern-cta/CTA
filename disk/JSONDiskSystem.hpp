/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "DiskSystem.hpp"
#include "common/json/object/JSONCObject.hpp"

using namespace cta::utils::json::object;

namespace cta { namespace disk {
  
class JSONDiskSystem : public DiskSystem, public JSONCObject {
public:
  JSONDiskSystem();
  JSONDiskSystem(const DiskSystem & diskSystem);
  void setAttributesFromJSON(const std::string & json) override;
  std::string getJSON() override;
  virtual ~JSONDiskSystem();
private:

};
}}


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

#include "DiskSystem.hpp"
#include "common/json/object/JSONCObject.hpp"

using namespace cta::utils::json::object;

namespace cta {
namespace disk {

/**
 * This class allows to transform a DiskSystem object into a JSON string
 * and to build a DiskSystem object from a JSON string
 */
class JSONDiskSystem : public DiskSystem, public JSONCObject {
public:
  JSONDiskSystem();
  JSONDiskSystem(const DiskSystem& diskSystem);
  /**
   * Builds the DiskSystem object with the json passed in parameter
   * @param json
   */
  void buildFromJSON(const std::string& json) override;
  /**
   * Get the json string representation of the inherited DiskSystem object
   * @return 
   */
  std::string getJSON() override;
  virtual ~JSONDiskSystem();

private:
};
}  // namespace disk
}  // namespace cta

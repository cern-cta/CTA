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

#include "common/json/object/JSONCObject.hpp"

namespace cta { namespace disk {
  
/**
 * This class allows  JSON-represent a FreeSpace object that is only a uint64_t value
 * {"freeSpace",42}
 */
class JSONFreeSpace: public cta::utils::json::object::JSONCObject {
public:
  JSONFreeSpace();
  void buildFromJSON(const std::string & json) override;
  std::string getJSON() override;
  std::string getExpectedJSONToBuildObject() const override;
  virtual ~JSONFreeSpace();
  uint64_t m_freeSpace = 0;
};

}}
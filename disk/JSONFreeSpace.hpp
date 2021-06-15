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
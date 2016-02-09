/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/MountType.hpp"

namespace cta {
namespace dataStructures {

class TapeMount {

public:

  /**
   * Constructor
   */
  TapeMount();

  /**
   * Destructor
   */
  ~TapeMount() throw();

  void setId(const uint64_t id);
  uint64_t getId() const;

  void setMountType(const cta::dataStructures::MountType &mountType);
  cta::dataStructures::MountType getMountType() const;

  void setVid(const std::string &vid);
  std::string getVid() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  uint64_t m_id;
  bool m_idSet;

  cta::dataStructures::MountType m_mountType;
  bool m_mountTypeSet;

  std::string m_vid;
  bool m_vidSet;

}; // class TapeMount

} // namespace dataStructures
} // namespace cta

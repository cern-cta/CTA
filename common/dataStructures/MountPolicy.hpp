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


namespace cta {
namespace common {
namespace dataStructures {

class MountPolicy {

public:

  /**
   * Constructor
   */
  MountPolicy();

  /**
   * Destructor
   */
  ~MountPolicy() throw();

  void setMaxDrives(const uint64_t maxDrives);
  uint64_t getMaxDrives() const;

  void setMinBytesQueued(const uint64_t minBytesQueued);
  uint64_t getMinBytesQueued() const;

  void setMinFilesQueued(const uint64_t minFilesQueued);
  uint64_t getMinFilesQueued() const;

  void setMinRequestAge(const uint64_t minRequestAge);
  uint64_t getMinRequestAge() const;

  void setPriority(const uint64_t priority);
  uint64_t getPriority() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  uint64_t m_maxDrives;
  bool m_maxDrivesSet;

  uint64_t m_minBytesQueued;
  bool m_minBytesQueuedSet;

  uint64_t m_minFilesQueued;
  bool m_minFilesQueuedSet;

  uint64_t m_minRequestAge;
  bool m_minRequestAgeSet;

  uint64_t m_priority;
  bool m_prioritySet;

}; // class MountPolicy

} // namespace dataStructures
} // namespace common
} // namespace cta

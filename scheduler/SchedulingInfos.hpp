/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include <string>

#include "SchedulerDatabase.hpp"


namespace cta {

/**
 * This class represents a scheduling information for a logical library, it is basically
 * a list of potential mounts (cta::SchedulerDatabase::PotentialMount object) for a given logical library.
 */
class SchedulingInfos {
public:
  /**
   * Constructor
   * @param logicalLibraryName the logical library 
   */
  SchedulingInfos(const std::string & logicalLibraryName);
  /**
   * Copy constructor
   */
  SchedulingInfos(const SchedulingInfos& other);
  /**
   * Assignment operator
   */
  SchedulingInfos & operator=(const SchedulingInfos & other);
  /**
   * Add a potential mount to this logical library scheduling informations
   * @param potentialMount the potential mount to add
   */
  void addPotentialMount(const cta::SchedulerDatabase::PotentialMount & potentialMount);
  
  /**
   * Returns the list of potential mounts for this logical library
   * @return a list of potential mounts for this logical library
   */
  std::list<cta::SchedulerDatabase::PotentialMount> getPotentialMounts() const;
  
  /**
   * Returns the logical library name of this SchedulingInfos instance
   * @return the logical library name of this SchedulingInfos instance
   */
  std::string getLogicalLibraryName() const;
  
  /**
   * Destructor
   */
  virtual ~SchedulingInfos();
private:
  std::string m_logicalLibraryName;
  std::list<cta::SchedulerDatabase::PotentialMount> m_potentialMounts;
};

}
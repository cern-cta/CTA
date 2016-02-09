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

#include "common/dataStructures/RetrieveJob.hpp"

namespace cta {
namespace dataStructures {

class RetrieveMount {

public:

  /**
   * Constructor
   */
  RetrieveMount();

  /**
   * Destructor
   */
  ~RetrieveMount() throw();

  void setJobs(const std::list<cta::dataStructures::RetrieveJob> &jobs);
  std::list<cta::dataStructures::RetrieveJob> getJobs() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::list<cta::dataStructures::RetrieveJob> m_jobs;
  bool m_jobsSet;

}; // class RetrieveMount

} // namespace dataStructures
} // namespace cta

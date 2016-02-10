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
#include <memory>
#include <stdint.h>
#include <string>

#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/TapeMount.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class ArchiveMount : public TapeMount {

public:

  /**
   * Constructor
   */
  ArchiveMount();

  /**
   * Destructor
   */
  ~ArchiveMount() throw();

  std::unique_ptr<cta::common::dataStructures::ArchiveJob> getNextJob();

  void setLastFSeq(const uint64_t lastFSeq);
  uint64_t getLastFSeq() const;
  
  void complete();

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  uint64_t m_lastFSeq;
  bool m_lastFSeqSet;

}; // class ArchiveMount

} // namespace dataStructures
} // namespace common
} // namespace cta

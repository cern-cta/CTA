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

#include "common/dataStructures/ArchiveRequest.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class ArchiveJob {

public:

  /**
   * Constructor
   */
  ArchiveJob();

  /**
   * Destructor
   */
  ~ArchiveJob() throw();

  void setArchiveFileID(const std::string &archiveFileID);
  std::string getArchiveFileID() const;

  void setCopyNumber(const std::string &copyNumber);
  std::string getCopyNumber() const;

  void setRequest(const cta::common::dataStructures::ArchiveRequest &request);
  cta::common::dataStructures::ArchiveRequest getRequest() const;

  void setTapePool(const std::string &tapePool);
  std::string getTapePool() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_archiveFileID;
  bool m_archiveFileIDSet;

  std::string m_copyNumber;
  bool m_copyNumberSet;

  cta::common::dataStructures::ArchiveRequest m_request;
  bool m_requestSet;

  std::string m_tapePool;
  bool m_tapePoolSet;

}; // class ArchiveJob

} // namespace dataStructures
} // namespace common
} // namespace cta

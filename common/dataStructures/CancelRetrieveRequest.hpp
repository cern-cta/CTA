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

#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/Requester.hpp"

namespace cta {
namespace dataStructures {

class CancelRetrieveRequest {

public:

  /**
   * Constructor
   */
  CancelRetrieveRequest();

  /**
   * Destructor
   */
  ~CancelRetrieveRequest() throw();

  void setArchiveFileID(const std::string &archiveFileID);
  std::string getArchiveFileID() const;

  void setDrData(const cta::dataStructures::DRData &drData);
  cta::dataStructures::DRData getDrData() const;

  void setDstURL(const std::string &dstURL);
  std::string getDstURL() const;

  void setRequester(const cta::dataStructures::Requester &requester);
  cta::dataStructures::Requester getRequester() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_archiveFileID;
  bool m_archiveFileIDSet;

  cta::dataStructures::DRData m_drData;
  bool m_drDataSet;

  std::string m_dstURL;
  bool m_dstURLSet;

  cta::dataStructures::Requester m_requester;
  bool m_requesterSet;

}; // class CancelRetrieveRequest

} // namespace dataStructures
} // namespace cta

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

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class RetrieveJob {

public:

  /**
   * Constructor
   */
  RetrieveJob();

  /**
   * Destructor
   */
  ~RetrieveJob() throw();

  void setRequest(const cta::common::dataStructures::RetrieveRequest &request);
  cta::common::dataStructures::RetrieveRequest getRequest() const;

  void setTapeCopies(const std::map<std::string,std::pair<int,cta::common::dataStructures::TapeFileLocation>> &tapeCopies);
  std::map<std::string,std::pair<int,cta::common::dataStructures::TapeFileLocation>> getTapeCopies() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  cta::common::dataStructures::RetrieveRequest m_request;
  bool m_requestSet;

  std::map<std::string,std::pair<int,cta::common::dataStructures::TapeFileLocation>> m_tapeCopies;
  bool m_tapeCopiesSet;

}; // class RetrieveJob

} // namespace dataStructures
} // namespace common
} // namespace cta

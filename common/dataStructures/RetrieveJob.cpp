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

#include "common/dataStructures/RetrieveJob.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveJob::RetrieveJob() {  
  m_requestSet = false;
  m_tapeCopiesSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveJob::~RetrieveJob() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::RetrieveJob::allFieldsSet() const {
  return m_requestSet
      && m_tapeCopiesSet;
}

//------------------------------------------------------------------------------
// setRequest
//------------------------------------------------------------------------------
void cta::common::dataStructures::RetrieveJob::setRequest(const cta::common::dataStructures::RetrieveRequest &request) {
  m_request = request;
  m_requestSet = true;
}

//------------------------------------------------------------------------------
// getRequest
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveRequest cta::common::dataStructures::RetrieveJob::getRequest() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveJob have been set!");
  }
  return m_request;
}

//------------------------------------------------------------------------------
// setTapeCopies
//------------------------------------------------------------------------------
void cta::common::dataStructures::RetrieveJob::setTapeCopies(const std::map<std::string,std::pair<int,cta::common::dataStructures::TapeFileLocation>> &tapeCopies) {
  m_tapeCopies = tapeCopies;
  m_tapeCopiesSet = true;
}

//------------------------------------------------------------------------------
// getTapeCopies
//------------------------------------------------------------------------------
std::map<std::string,std::pair<int,cta::common::dataStructures::TapeFileLocation>> cta::common::dataStructures::RetrieveJob::getTapeCopies() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveJob have been set!");
  }
  return m_tapeCopies;
}

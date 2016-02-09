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

#include "common/dataStructures/RetrieveMount.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::RetrieveMount::RetrieveMount() {  
  m_jobsSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::RetrieveMount::~RetrieveMount() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::RetrieveMount::allFieldsSet() const {
  return m_jobsSet;
}

//------------------------------------------------------------------------------
// setJobs
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveMount::setJobs(const std::list<cta::dataStructures::RetrieveJob> &jobs) {
  m_jobs = jobs;
  m_jobsSet = true;
}

//------------------------------------------------------------------------------
// getJobs
//------------------------------------------------------------------------------
std::list<cta::dataStructures::RetrieveJob> cta::dataStructures::RetrieveMount::getJobs() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveMount have been set!");
  }
  return m_jobs;
}

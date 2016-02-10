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
cta::common::dataStructures::RetrieveMount::RetrieveMount() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::RetrieveMount::~RetrieveMount() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::RetrieveMount::allFieldsSet() const {
  return true;
}

//------------------------------------------------------------------------------
// getNextJob
//------------------------------------------------------------------------------
std::unique_ptr<cta::common::dataStructures::RetrieveJob> cta::common::dataStructures::RetrieveMount::getNextJob() {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveMount have been set!");
  }
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
  return std::unique_ptr<cta::common::dataStructures::RetrieveJob>();
}

//------------------------------------------------------------------------------
// diskComplete
//------------------------------------------------------------------------------
void cta::common::dataStructures::RetrieveMount::diskComplete() {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveMount have been set!");
  }
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
}

//------------------------------------------------------------------------------
// tapeComplete
//------------------------------------------------------------------------------
void cta::common::dataStructures::RetrieveMount::tapeComplete() {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveMount have been set!");
  }
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
}

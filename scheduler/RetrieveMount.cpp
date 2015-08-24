/*
 * The CERN Tape Retrieve (CTA) project
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

#include "scheduler/RetrieveMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount(
  std::unique_ptr<SchedulerDatabase::RetrieveMount> dbMount) {
  m_dbMount.reset(dbMount.release());
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::MountType::Enum cta::RetrieveMount::getMountType() const throw() {
  return MountType::ARCHIVE;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVid() const throw() {
  return "UNKNOWN_VID_FOR_RETRIEVE_MOUNT";
}

//------------------------------------------------------------------------------
// getDensity
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getDensity() const throw() {
  return "UNKNOWN_DENSITY_FOR_RETRIEVE_MOUNT";
}

//------------------------------------------------------------------------------
// getMountTransactionId
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getMountTransactionId() const throw(){
  return "UNKNOWN_MOUNTTRANSACTIONID_FOR_RETRIEVE_MOUNT";
}

//------------------------------------------------------------------------------
// getNextJob
//------------------------------------------------------------------------------
std::unique_ptr<cta::RetrieveJob> cta::RetrieveMount::getNextJob() {
  throw NotImplemented(std::string(__FUNCTION__) + ": Not implemented");
}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::RetrieveMount::complete() {
  throw NotImplemented(std::string(__FUNCTION__) + ": Not implemented");
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveMount::~RetrieveMount() throw() {
}

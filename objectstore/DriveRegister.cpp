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

#include "DriveRegister.hpp"
#include "ProtcolBuffersAlgorithms.hpp"

cta::objectstore::DriveRegister::DriveRegister(const std::string & address, Backend & os):
  ObjectOps<serializers::DriveRegister>(os, address) { }

void cta::objectstore::DriveRegister::initialize() {
  // Setup underlying object
  ObjectOps<serializers::DriveRegister>::initialize();
  m_payloadInterpreted = true;
}

void cta::objectstore::DriveRegister::addDrive(std::string driveName) {
  checkPayloadWritable();
  // Check that we are not trying to duplicate a drive
  try {
    serializers::findElement(m_payload.drivenames(), driveName);
    throw DuplicateEntry("In DriveRegister::addDrive: entry already exists");
  } catch (serializers::NotFound &) {}
  m_payload.add_drivenames(driveName);
}

bool cta::objectstore::DriveRegister::isEmpty() {
  checkPayloadReadable();
  if (m_payload.drivenames_size())
    return false;
  return true;
}





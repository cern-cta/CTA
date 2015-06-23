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

#include "Tape.hpp"
#include "GenericObject.hpp"

cta::objectstore::Tape::Tape(const std::string& address, Backend& os):
  ObjectOps<serializers::Tape>(os, address) { }

cta::objectstore::Tape::Tape(GenericObject& go):
  ObjectOps<serializers::Tape>(go.objectStore()){
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void cta::objectstore::Tape::initialize(const std::string &name) {
  ObjectOps<serializers::Tape>::initialize();
  // Set the reguired fields
  m_payload.set_vid(name);
  m_payload.set_bytesstored(0);
  m_payload.set_lastfseq(0);
  m_payloadInterpreted = true;
}


bool cta::objectstore::Tape::isEmpty() {
  checkPayloadReadable();
  return !m_payload.retrievaljobs_size();
}

void cta::objectstore::Tape::removeIfEmpty() {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("In Tape::removeIfEmpty: trying to remove an tape with retrievals queued");
  }
  remove();
}

void cta::objectstore::Tape::addStoredData(uint64_t bytes) {
  checkPayloadWritable();
  m_payload.set_bytesstored(m_payload.bytesstored()+bytes);
}

uint64_t cta::objectstore::Tape::getStoredData() {
  checkPayloadReadable();
  return m_payload.bytesstored();
}

std::string cta::objectstore::Tape::getVid() {
  checkPayloadReadable();
  return m_payload.vid();
}

std::string cta::objectstore::Tape::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "<<<< Tape dump start: vid=" << m_payload.vid() << std::endl;
  ret << "  lastFseq=" << m_payload.lastfseq() 
      << " bytesStored=" << m_payload.bytesstored() << std::endl;
  ret << "  Retrieval jobs queued: " << m_payload.retrievaljobs_size() << std::endl;
  if (m_payload.readmounts_size()) {
    auto lrm = m_payload.readmounts(0);
    ret << "  Latest read for mount: " << lrm.host() << " " << lrm.time() << " " 
        << lrm.drivevendor() << " " << lrm.drivemodel() << " " 
        << lrm.driveserial() << std::endl;
  }
  if (m_payload.writemounts_size()) {
    auto lwm = m_payload.writemounts(0);
    ret << "  Latest write for mount: " << lwm.host() << " " << lwm.time() << " " 
        << lwm.drivevendor() << " " << lwm.drivemodel() << " " 
        << lwm.driveserial() << std::endl;
  }
  ret << ">>>> Tape dump end" << std::endl;
  return ret.str();
}






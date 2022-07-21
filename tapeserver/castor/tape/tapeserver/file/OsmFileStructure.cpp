/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "castor/tape/tapeserver/file/OsmFileStructure.hpp"
#include "tapeserver/castor/tape/tapeserver/file/OsmXdrStructure.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "common/exception/Exception.hpp"

#include <rpc/xdr.h> // xdrmem_create for decode_legato
#include <cstring> // memeset
#include <string> // std::string to_string

castor::tape::tapeFile::osm::LABEL::LABEL() {
  // Fill the structre with space & 0
  memset(m_tcRawLabel, 0x20, sizeof(m_tcRawLabel));
  memset(m_tcVersion, 0x20, sizeof(m_tcVersion));
  memset(m_tcName, 0x20, sizeof(m_tcName));
  memset(m_tcOwner, 0x20, sizeof(m_tcOwner));
  m_ulCreateTime = 0;
  m_ulExpireTime = 0; 
  m_ulRecSize = 0;
  m_ulVolId = 0;
}
/**
 *
 */
void castor::tape::tapeFile::osm::LABEL::decode() {
  xdr::Record record;
  xdr::VolLabel volLabel, *pVolLabel;
  XDR xdr;// xdr handle;
  
  xdrmem_create(&xdr, rawLabel(), LIMITS::MAXMRECSIZE, XDR_DECODE);
  if(!record.decode(&xdr)) {
    throw cta::exception::Exception(std::string("XDR error getting record"));
  }
  if(record.m_RChunk.m_pChunk == nullptr) {
    throw cta::exception::Exception(std::string("Invalid label format - no record chunk"));
  }
  
  pVolLabel = reinterpret_cast<xdr::VolLabel*>(record.m_RChunk.m_pChunk->m_data.m_pcDataVal);
  xdr_destroy(&xdr);
  
  if(pVolLabel == nullptr) {
    throw cta::exception::Exception(std::string("Invalid label format - no label chunk"));
  }
  xdrmem_create(&xdr, reinterpret_cast<char*>(pVolLabel), LIMITS::MMAXCHK, XDR_DECODE);
  volLabel.m_pcVolName = nullptr;    // let XDR fool with this
  if(!volLabel.decode(&xdr)) {
    throw cta::exception::Exception(std::string("XDR error getting vollabel"));
  }

  xdr_destroy(&xdr);
  // we're done with this now

  if (volLabel.m_ulMagic != LIMITS::MVOLMAGIC) {
     throw cta::exception::Exception(std::string("magic number " + std::to_string(volLabel.m_ulMagic) + " not valid"));
  }

  if (volLabel.m_pcVolName == nullptr) {
     throw cta::exception::Exception(std::string("Invalid label format - no volume name"));
  }
  if (strlen(volLabel.m_pcVolName) >= LIMITS::VOLNAMELEN) {
     throw cta::exception::Exception(std::string("label " + std::string(volLabel.m_pcVolName) + " is too long"));
  }
  
  strcpy(m_tcName, volLabel.m_pcVolName);
  m_ulCreateTime = volLabel.m_ulCreateTime;
  m_ulExpireTime = volLabel.m_ulExpireTime;
  m_ulRecSize = volLabel.m_ulRecSize;
  m_ulVolId = volLabel.m_ulVolId;
  
  memcpy(m_tcOwner, rawLabel() + LIMITS::MAXMRECSIZE, LIMITS::CIDLEN);
  memcpy(m_tcVersion, rawLabel() + LIMITS::MAXMRECSIZE + LIMITS::CIDLEN, LIMITS::LABELVERSIONLEN);

}

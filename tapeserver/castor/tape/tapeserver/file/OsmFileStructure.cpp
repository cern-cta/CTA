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

  xdr_destroy(&xdr); // we're done with this now

  if (volLabel.m_ulMagic != LIMITS::MVOLMAGIC) {
     throw cta::exception::Exception(std::string("magic number " + std::to_string(volLabel.m_ulMagic) + " not valid"));
  }
  if (volLabel.m_pcVolName == nullptr) {
     throw cta::exception::Exception(std::string("Invalid label format - no volume name"));
  }
  m_tcName[LIMITS::VOLNAMELEN] = 0;
  strncpy(m_tcName, volLabel.m_pcVolName, LIMITS::VOLNAMELEN+1);
  if(m_tcName[LIMITS::VOLNAMELEN] != 0) {
    throw cta::exception::Exception(std::string("label ") + std::string(volLabel.m_pcVolName) + " is too long");
  }
  m_ulCreateTime = volLabel.m_ulCreateTime;
  m_ulExpireTime = volLabel.m_ulExpireTime;
  m_ulRecSize = volLabel.m_ulRecSize;
  m_ulVolId = volLabel.m_ulVolId;
  
  memcpy(m_tcOwner, rawLabel() + LIMITS::MAXMRECSIZE, LIMITS::CIDLEN);
  memcpy(m_tcVersion, rawLabel() + LIMITS::MAXMRECSIZE + LIMITS::CIDLEN, LIMITS::LABELVERSIONLEN);
}

void castor::tape::tapeFile::osm::LABEL::encode(uint64_t ulCreateTime, uint64_t ulExpireTime, uint64_t ulRecSize, uint64_t ulVolId,
                                                const std::string& strVolName, const std::string& strOwner, const std::string& strVersion) {

  if(strVolName.size() > LIMITS::VOLNAMELEN) {
    throw cta::exception::Exception(std::string("The size of the VolName is greater than LIMITS::VOLNAMELEN"));
  }
  if(strOwner.size() > LIMITS::CIDLEN) {
    throw cta::exception::Exception(std::string("The size of the Owner is greater than LIMITS::CIDLEN"));
  }
  if(strVersion.size() > LIMITS::LABELVERSIONLEN) {
    throw cta::exception::Exception(std::string("The size of the Version is greater than LIMITS::LABELVERSIONLEN"));
  }

  unsigned int uiDataLen = 0;
  xdr::Chunk chunk;
  xdr::Record record;
  xdr::VolLabel volLabel;
  char* pcVolLabel = new char[LIMITS::MMAXCHK];
  XDR xdr;// xdr handler

  xdrmem_create(&xdr, pcVolLabel, LIMITS::MMAXCHK, XDR_ENCODE);

  volLabel.m_ulMagic = LIMITS::MVOLMAGIC;
  volLabel.m_ulCreateTime = ulCreateTime;
  volLabel.m_ulExpireTime = ulExpireTime;
  volLabel.m_ulRecSize = ulRecSize;// LIMITS::MAXMRECSIZE;
  volLabel.m_ulVolId = ulVolId;
  // NSR_LENGTH = 64
  volLabel.m_pcVolName = new char[64];
  strncpy(volLabel.m_pcVolName, strVolName.c_str(), LIMITS::VOLNAMELEN+1);

  if(!volLabel.decode(&xdr)) {
    throw cta::exception::Exception(std::string("XDR error encoding vollabel"));
  }

  uiDataLen = xdr_getpos(&xdr);

  xdr_destroy(&xdr);

  record.m_RChunk.m_pChunk = new xdr::Chunk();
  record.m_RChunk.m_pChunk->m_ulSsid = 0;
  record.m_RChunk.m_pChunk->mc_ulLow = 0;
  record.m_RChunk.m_pChunk->m_data.m_uiDataLen = uiDataLen;
  record.m_RChunk.m_pChunk->m_data.m_pcDataVal = pcVolLabel;//new char[LIMITS::MMAXCHK];

  memset(record.m_tcHandler, 0x20, sizeof(record.m_tcHandler));
  record.m_ulVolid = volLabel.m_ulVolId;
  record.m_ulFn = 0;
  record.m_ulRn = 0;
  record.m_ulLen = 0; // set this to 0 now, it will be corrected later
  record.m_RChunk.m_ulChunkLen = 1;

  xdrmem_create(&xdr, rawLabel(), LIMITS::MAXMRECSIZE, XDR_ENCODE);
  if(!record.decode(&xdr)) {
    throw cta::exception::Exception(std::string("XDR error encoding record"));
  }

  record.m_ulLen  = xdr_getpos(&xdr); // Now re-do this with the correct length

  xdr_setpos(&xdr, 0);
  if(!record.decode(&xdr)) {
    throw cta::exception::Exception(std::string("XDR error encoding record after set pos"));
  }

  xdr_destroy(&xdr);

  strncpy(rawLabel() + LIMITS::MAXMRECSIZE, strOwner.c_str(), LIMITS::CIDLEN+1);
  strncpy(rawLabel() + LIMITS::MAXMRECSIZE + LIMITS::CIDLEN, strVersion.c_str(), LIMITS::LABELVERSIONLEN+1);
}

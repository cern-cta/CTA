/******************************************************************************
 *                      castor/stager/Segment.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::Segment::Segment() throw() :
  m_blockid(),
  m_fseq(0),
  m_offset(),
  m_bytes_in(),
  m_bytes_out(),
  m_host_bytes(),
  m_segmCksumAlgorithm(""),
  m_segmCksum(0),
  m_errMsgTxt(""),
  m_errorCode(0),
  m_severity(0),
  m_id(),
  m_tape(0),
  m_copy(0),
  m_status(SegmentStatusCodes(0)) {
  memset(m_blockid, 0, 4 * sizeof(unsigned char));
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::Segment::~Segment() throw() {
  if (0 != m_tape) {
    m_tape->removeSegments(this);
  }
  if (0 != m_copy) {
    m_copy->removeSegments(this);
  }
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Segment::print(std::ostream& stream,
                                    std::string indent,
                                    castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# Segment #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "blockid : " << m_blockid << std::endl;
  stream << indent << "fseq : " << m_fseq << std::endl;
  stream << indent << "offset : " << m_offset << std::endl;
  stream << indent << "bytes_in : " << m_bytes_in << std::endl;
  stream << indent << "bytes_out : " << m_bytes_out << std::endl;
  stream << indent << "host_bytes : " << m_host_bytes << std::endl;
  stream << indent << "segmCksumAlgorithm : " << m_segmCksumAlgorithm << std::endl;
  stream << indent << "segmCksum : " << m_segmCksum << std::endl;
  stream << indent << "errMsgTxt : " << m_errMsgTxt << std::endl;
  stream << indent << "errorCode : " << m_errorCode << std::endl;
  stream << indent << "severity : " << m_severity << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "Tape : " << std::endl;
  if (0 != m_tape) {
    m_tape->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "Copy : " << std::endl;
  if (0 != m_copy) {
    m_copy->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "status : " << SegmentStatusCodesStrings[m_status] << std::endl;
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Segment::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::Segment::TYPE() {
  return OBJ_Segment;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::stager::Segment::setId(u_signed64 id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
u_signed64 castor::stager::Segment::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::Segment::type() const {
  return TYPE();
}


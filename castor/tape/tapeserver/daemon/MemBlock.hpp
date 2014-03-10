/******************************************************************************
 *                      MemBlock.hpp
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/tape/tapeserver/daemon/Exception.hpp"
#include <memory>

class MemBlock {
public:

  MemBlock(const int id, const size_t capacity) throw(MemException) : m_memoryBlockId(id), m_payload(new (std::nothrow) char[capacity]), m_payload_capacity(capacity) {
    if(m_payload.get()==NULL) {
      throw MemException("Failed to allocate memory for a new MemBlock!");
    }
    reset();
  }
  
  void reset() throw() {
    m_fileid = -1;
    m_fileBlock = -1;
    m_fSeq = -1;
    m_tapeFileBlock = -1;
  }
  
  int m_memoryBlockId;
  
  int m_fileid;
  int m_fileBlock;
  int m_fSeq;
  int m_tapeFileBlock;
  
  std::auto_ptr<char> m_payload;
  size_t m_payload_capacity;
  size_t m_payload_size;
};

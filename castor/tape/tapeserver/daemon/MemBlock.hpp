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
#include "castor/tape/tapeserver/file/File.hpp"
#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

class Payload
{
public:
  Payload(size_t capacity):
  m_payload(new (std::nothrow) char[capacity]),m_capacity(capacity),m_size(0) {
    if(NULL == m_payload) {
      throw MemException("Failed to allocate memory for a new MemBlock!");
    }
  }
  ~Payload(){
    delete[] m_payload;
  }
  
  size_t size() const {
    return m_size;
  }
  
  size_t capacity() const {
    return m_capacity;
  }
  
  //TODO interface TBD
  char* get(){
    return m_payload;
  }
  
  char const*  get() const {
    return m_payload;
  }
  
  void read(tape::diskFile::ReadFile& from){
    m_size = from.read(m_payload,m_capacity);
  }
  
  void write(tape::diskFile::WriteFile& to){
    to.write(m_payload,m_size);
  }
    
private:
  char* m_payload;
  size_t m_capacity;
  size_t m_size;
};



class MemBlock {
public:
  static const int uninitialised_value = -1;
  MemBlock(const int id, const size_t capacity) : 
  m_memoryBlockId(id),m_payload(capacity){
    reset();
  }
  
  void reset() throw() {
    m_fileid = uninitialised_value;
    m_fileBlock = uninitialised_value;
    m_fSeq = uninitialised_value;
    m_tapeFileBlock = uninitialised_value;
  }
  //identify block id  
  const int m_memoryBlockId;
  
  //handle the raw data to be migrated/recalled
  Payload m_payload;
  
  //castor-internal unique id of the file
  int m_fileid;

  //number of the memory-chunk of the current file we are manipulating
  int m_fileBlock;
  
  //order of file on the tape
  int m_fSeq;
  
  //number of the memory-tape-chunk of the current file we are manipulating
  int m_tapeFileBlock;
 
};

}
}
}
}

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

/**
 * Class managing a fixed size payload buffer. Some member functions also
 * allow read
 * @param capacity Size of the payload buffer in bytes
 */
class Payload
{
public:
  Payload(size_t capacity):
  m_payload(new (std::nothrow) char[capacity]),m_totalCapacity(capacity),m_size(0) {
    if(NULL == m_payload) {
      throw MemException("Failed to allocate memory for a new MemBlock!");
    }
  }
  ~Payload(){
    delete[] m_payload;
  }
  
  /** Amount of data present in the payload buffer */
  size_t size() const {
    return m_size;
  }
  
  /** Remaining free space in the payload buffer */
  size_t remainingFreeSpace() const {
    return m_totalCapacity - m_size;
  }
  
  /** Total size of the payload block */
  size_t totalCapacity() const {
    return m_totalCapacity;
  }
  
  /** Returns a pointer to the beginning of the payload block */
  char* get(){
    return m_payload;
  }
  
  /** Returns a pointer to the beginning of the payload block (readonly version) */
  char const*  get() const {
    return m_payload;
  }
  
  /** 
   * Reads all the buffer in one go from a diskFile::ReadFile object 
   * @param from reference to the diskFile::ReadFile
   */
  void read(tape::diskFile::ReadFile& from) {
    m_size = from.read(m_payload,m_totalCapacity);
  }
  
  class EndOfFile: public castor::exception::Exception {
  public:
    EndOfFile(const std::string & w): castor::exception::Exception(w) {}
    virtual ~EndOfFile() throw() {}
  };
  
  /**
   * Reads one block from a tapeFile::readFile
   * @throws castor::tape::daemon::Payload::EOF
   * @param from reference to the tapeFile::ReadFile
   * @return whether another tape block will fit in the memory block.
   */
  bool append(tape::tapeFile::ReadFile & from){
    if (from.getBlockSize() > remainingFreeSpace()) {
      std::stringstream err;
      err << "Trying to read a tape file block with too little space left: BlockSize="
       << from.getBlockSize() << " remainingFreeSpace=" << remainingFreeSpace()
              << " (totalSize=" << m_totalCapacity << ")"; 
      throw MemException(err.str());
    }
    size_t readSize;
    try {
      readSize = from.read(m_payload + m_size, from.getBlockSize());
    } catch (castor::tape::tapeFile::EndOfFile) {
      throw EndOfFile("In castor::tape::tapeserver::daemon::Payload::append: reached end of file");
    }
    m_size += readSize;
    return  from.getBlockSize() <= remainingFreeSpace();
  }
  
  /**
   * Write the complete buffer to a diskFile::WriteFile
   * @param to reference to the diskFile::WriteFile
   */
  void write(tape::diskFile::WriteFile& to){
    to.write(m_payload,m_size);
  }
  
  /**
   * Write the complete buffer to a tapeFile::WriteFile, tape block by
   * tape block
   * @param to reference to the tapeFile::WriteFile
   */
  void write(tape::tapeFile::WriteFile& to) {
    size_t blockSize = to.getBlockSize();
    size_t writePosition = 0;
    // Write all possible full tape blocks
    while (m_size - writePosition > blockSize) {
      to.write(m_payload + writePosition, blockSize);
      writePosition += blockSize;
    }
    // Write a remainder, if any
    if (m_size - writePosition) {
      to.write(m_payload + writePosition, m_size - writePosition);
    }
  }
    
private:
  char* m_payload;
  size_t m_totalCapacity;
  size_t m_size;
};


/**
 * Individual memory block with metadata
 */
class MemBlock {
public:
  static const int uninitialised_value = -1;
  MemBlock(const int id, const size_t capacity) : 
  m_memoryBlockId(id),m_payload(capacity){
    reset();
  }
  
  /**
   * Reset the values of all the 
   */
  void reset() throw() {
    m_fileid = uninitialised_value;
    m_fileBlock = uninitialised_value;
    m_fSeq = uninitialised_value;
    m_tapeFileBlock = uninitialised_value;
    m_failed=false;
  }
  /** Unique memory block id */
  const int m_memoryBlockId;
  
  /** handle to the raw data to be migrated/recalled */
  Payload m_payload;
  
  /** CASTOR NsFileId file concerned */
  uint64_t m_fileid;

  /** number of the memory-chunk of the current file we are manipulating */
  int m_fileBlock;
  
  /** order of file on the tape */
  uint32_t m_fSeq;
  
  /** Sequence number of the first tape block file in this memory block */
  int m_tapeFileBlock;
  
  /** Size of the tape blocks, allowing sanity checks on the disk write side in recalls */
  int m_tapeBlockSize;
  
  /** Flag indicating to the receiver that the file read failed */
  bool m_failed;
 
};

}
}
}
}

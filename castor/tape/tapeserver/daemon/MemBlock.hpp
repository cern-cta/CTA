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

#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include <memory>
#include "castor/tape/tapeserver/daemon/Payload.hpp"


namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
/**
 * Individual memory block with metadata
 */
class MemBlock {
public:
  /**
   * COnstrucor 
   * @param id the block ID for its whole life
   * @param capacity the capacity (in byte) of the embed payload 
   */
  MemBlock(const int id, const size_t capacity) : 
  m_memoryBlockId(id),m_payload(capacity){
    reset();
  }
  
  /**
   * Mark this block as failed ie 
   * m_failed is true, m_fileBlock and m_tapeFileBlock are set at -1
   * Other members do not change
   */
  void markAsFailed(){
    m_failed = true;
    m_fileBlock = -1;
    m_tapeFileBlock = -1;
  }
  /**
   * Reset all the members.
   * Numerical ones are set at -1.and m_failed to false.
   */
  void reset() throw() {
    m_fileid = -1;
    m_fileBlock = -1;
    m_fSeq = -1;
    m_tapeFileBlock = -1;
    m_failed=false;
    m_payload.reset();
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


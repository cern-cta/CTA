/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "castor/tape/tapeserver/file/File.hpp"
#include "common/exception/Exception.hpp"
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
  
  struct AlterationContext{
    //provide an enumation of type, thus we can 
    //overload the constructor on those types
    struct Failed_t{};
    static Failed_t Failed ;
    
    struct Cancelled_t{}; 
    static Cancelled_t Cancelled;

    struct VerifyOnly_t{};
    static VerifyOnly_t VerifyOnly;

    /** Flag indicating to the receiver that the file read failed */
    bool m_failed;
    
    /** Flag indicating that the transfer was cancelled, usually due to a 
     previous failure. */
    bool m_cancelled;

    /** Flag indicating that the transfer is verify only, no disk file
     should be written. */
    bool m_verifyonly;

    /**
     * in case of error, the error message 
     */
    std::string m_errorMsg;
    
    /**
     * in case of error, the error message 
     */
    int m_errorCode;
    
    AlterationContext(const std::string& msg,int errorCode,Failed_t):
    m_failed(true),m_cancelled(false),m_verifyonly(false),m_errorMsg(msg),m_errorCode(errorCode){}
    
    AlterationContext(Cancelled_t):
    m_failed(false),m_cancelled(true),m_verifyonly(false),m_errorMsg(""),m_errorCode(0){}
    
    AlterationContext(VerifyOnly_t):
    m_failed(false),m_cancelled(false),m_verifyonly(true),m_errorMsg(""),m_errorCode(0){}
  };
  
  std::unique_ptr<AlterationContext> m_context;
public:
  /**
   * Constructor 
   * @param id the block ID for its whole life
   * @param capacity the capacity (in byte) of the embed payload 
   */
  MemBlock(const int id, const size_t capacity) : 
  m_memoryBlockId(id),m_payload(capacity){
    reset();
  }
  
  /**
   * Get the error message from the context, 
   * Throw an exception if there is no context
   * @return 
   */
  std::string errorMsg() const {
    if(m_context.get()) {
      return m_context->m_errorMsg;
    }

    throw cta::exception::Exception("Error Context is not set ="
            " no error message to give");
  }

  /**
   * Get the error code from the context, 
   * Throw an exception if there is no context
   * @return 
   */
  int errorCode() const {
    if(m_context.get()) {
      return m_context->m_errorCode;
    }

    throw cta::exception::Exception("Error Context is not set ="
            " no error code to give");
  }
  
  /**
   * Return true if the block has been marked as failed 
   * @return 
   */
  bool isFailed() const {
    return m_context.get() && m_context->m_failed;
  }
  
  /**
   * Return true if the block has been marked as canceled 
   * @return 
   */
  bool isCanceled() const {
    return m_context.get() && m_context->m_cancelled;
  }
    
  /**
   * Return true if the block has been marked as verify only
   * @return 
   */
  bool isVerifyOnly() const {
    return m_context.get() && m_context->m_verifyonly;
  }
    
  /**
   * Mark this block as failed ie 
   * m_failed is true, m_fileBlock and m_tapeFileBlock are set at -1
   * Other members do not change
   */
  void markAsFailed(const std::string msg,int errCode){
    m_context.reset(new AlterationContext(msg,errCode,AlterationContext::Failed));
    m_fileBlock = -1;
    m_tapeFileBlock = -1;
  }
  /**
   * Mark the block as canceled: this indicates the writer thread that
   * the read was skipped due to previous, unrelated errors, and that this
   * file will not be processed at all (and hence should not be reported about).
   * This is mainly used for the tape read case, when positioning is confused
   * (when positioning by fSeq, there's nothing we can do).
   */
  void markAsCancelled(){
    m_context.reset(new AlterationContext(AlterationContext::Cancelled));
    m_fileBlock = -1;
    m_tapeFileBlock = -1;
  }
  /**
   * Mark the block as verify only: no disk file will be written but the
   * file should otherwise be processed normally
   */
  void markAsVerifyOnly(){
    m_context.reset(new AlterationContext(AlterationContext::VerifyOnly));
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
    m_payload.reset();
    
    //delete the previous m_context (if allocated) 
    //and set the new one to nullptr
    m_context.reset();
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
  
};

}
}
}
}


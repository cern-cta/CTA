/******************************************************************************
 *                      TapeReadFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"


namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
class TapeReadFileTask: public TapeReadTask {
public:
  TapeReadFileTask(castor::tape::tapegateway::FileToRecallStruct * ftr,
    DataConsumer & destination, RecallMemoryManager & mm): 
    m_fileToRecall(ftr), m_fifo(destination), m_mm(mm) {}
  /* Implementation of the TapeReadTask interface*/
  virtual bool endOfWork() { return false; }
  virtual void execute(castor::tape::tapeFile::ReadSession & rs,
    castor::log::LogContext & lc) {
    using castor::log::Param;
    typedef castor::log::LogContext::ScopedParam ScopedParam;
    // Placeholder for the tape file read
    
    // Set the common context for all the omming logs (file info)
    ScopedParam sp0(lc, Param("NSHOSTNAME", m_fileToRecall->nshost()));
    ScopedParam sp1(lc, Param("NSFILEID", m_fileToRecall->fileid()));
    ScopedParam sp2(lc, Param("BlockId", castor::tape::tapeFile::BlockId::extract(*m_fileToRecall)));
    ScopedParam sp3(lc, Param("fSeq", m_fileToRecall->fseq()));
    ScopedParam sp4(lc, Param("fileTransactionId", m_fileToRecall->fileTransactionId()));
    
    // Read the file and transmit it
    bool stillReading = true;
    int fileBlock = 0;
    int tapeBlock = 0;
    try {
      std::auto_ptr<castor::tape::tapeFile::ReadFile> rf(openReadFile(rs,lc));
      while (stillReading) {
        // Get a memory block and add information to its metadata
        std::auto_ptr<MemBlock> mb(m_mm.getFreeBlock());
        mb->m_fSeq = m_fileToRecall->fseq();
        mb->m_failed = false;
        mb->m_fileBlock = fileBlock++;
        mb->m_fileid = m_fileToRecall->fileid();
        mb->m_tapeFileBlock = tapeBlock;
        mb->m_tapeBlockSize = rf->getBlockSize();
        try {
          // Fill up the memory block with tape block
          // append conveniently returns false when there will not be more space
          // for an extra tape block, and throws an exception if we reached the
          // end of file. append() also protects against reading too big tape blocks.
          while (mb->m_payload.append(*rf)) {
          tapeBlock++;
          }
          // Pass the block to the disk write task
          m_fifo.pushDataBlock(mb.release());
        } catch (const castor::tape::exceptions::EndOfFile&) {
          // append() signaled the end of the file.
          stillReading = false;
        } 
        
      } //end of while(stillReading)
    
      } //end of try
      catch (castor::exception::Exception & ex) {
        //we end there because :
        //openReadFile brought us here (cant put the tape into position)
        //m_payload.append brought us here (error while reading the file)
        // This is an error case. Log and signal to the disk write task
        { 
          castor::log::LogContext::ScopedParam sp0(lc, Param("fileBlock", fileBlock));
          castor::log::LogContext::ScopedParam sp1(lc, Param("ErrorMessage", ex.getMessageValue()));
          castor::log::LogContext::ScopedParam sp2(lc, Param("ErrorCode", ex.code()));
          lc.log(LOG_ERR, "Error reading a file block in TapeReadFileTask (backtrace follows)");
        }
        {
          castor::log::LogContext lc2(lc.logger());
          lc2.logBacktrace(LOG_ERR, ex.backtrace());
        }
        
        //write the block 
        std::auto_ptr<MemBlock> mb(m_mm.getFreeBlock());
        mb->m_fSeq = m_fileToRecall->fseq();
        mb->m_failed = true;
        mb->m_fileBlock = -1;
        mb->m_fileid = m_fileToRecall->fileid();
        mb->m_tapeFileBlock = -1;
        m_fifo.pushDataBlock(mb.release());
        m_fifo.pushDataBlock(NULL);
        return;
      }
    // In all cases, we have to signal the end of the tape read to the disk write
    // task.
    m_fifo.pushDataBlock(NULL);
    lc.log(LOG_DEBUG, "File read completed");
  }
private:
  
  // Open the file and manage failure (if any)
  std::auto_ptr<castor::tape::tapeFile::ReadFile> openReadFile(
  castor::tape::tapeFile::ReadSession & rs, castor::log::LogContext & lc){

    using castor::log::Param;
    typedef castor::log::LogContext::ScopedParam ScopedParam;

    std::auto_ptr<castor::tape::tapeFile::ReadFile> rf;
    try {
      rf.reset(new castor::tape::tapeFile::ReadFile(&rs, *m_fileToRecall));
      lc.log(LOG_DEBUG, "Successfully opened the tape file");
    } catch (castor::exception::Exception & ex) {
      // Log the error
      ScopedParam sp0(lc, Param("ErrorMessage", ex.getMessageValue()));
      ScopedParam sp1(lc, Param("ErrorCode", ex.code()));
      lc.log(LOG_ERR, "Failed to open tape file for reading");
      throw;
    }
    return rf;
  }
  std::auto_ptr<castor::tape::tapegateway::FileToRecallStruct> m_fileToRecall;
  DataConsumer & m_fifo;
  RecallMemoryManager & m_mm;

};
}
}
}
}
 

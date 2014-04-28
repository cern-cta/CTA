/******************************************************************************
 *                      TapeWriteFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/TapeWriteTaskInterface.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * The TapeWriteFileTask is responsible to write a single file onto tape as part of a migration
 * session. Being a consumer of memory blocks, it inherits from the DataConsumer class. It also
 * inherits several methods from the TapeWriteTask (TODO: do we really need this base class?).
 */

class TapeWriteTask: public TapeWriteTaskInterface, public DataConsumer {
public:
  /**
   * Constructor
   * @param fSeq: file sequence number of the file to be written on tape
   * @param blockCount: number of memory blocks (TODO:?)
   * @param mm: reference to the memory manager in use
   */
  TapeWriteTask(int blockCount, tape::tapegateway::FileToMigrateStruct* file,
          MigrationMemoryManager& mm,castor::tape::threading::AtomicFlag& errorFlag);
  
  
  /**
   * @return the size of the file in byte
   */
  virtual int fileSize();
    
  /**
   * Main execution routine
   * @param td: tape drive object which will handle the file
   */
  virtual void execute(castor::tape::tapeFile::WriteSession & session,
   MigrationReportPacker & reportPacker,castor::log::LogContext& lc);
  
  /**
   * Used to reclaim used memory blocks
   * @return the recyclable memory block
   */
  virtual MemBlock * getFreeBlock();
  
  /**
   * This is to enqueue one memory block full of data to be written on tape
   * @param mb: the memory block in question
   */
  virtual void pushDataBlock(MemBlock *mb) ;
  
  /**
   * Destructor
   */
  virtual ~TapeWriteTask();
  
private:
    void hasAnotherTaskTailed() const {
    //if a task has signaled an error, we stop our job
    if(m_errorFlag){
      throw  castor::tape::exceptions::ErrorFlag();
    }
  }
    
  void circulateMemBlocks();
  /**
   * Function in charge of opening the WriteFile for m_fileToMigrate
   * Throw an exception it it fails
   * @param session The session on which relies the WriteFile
   * @param lc for logging purpose
   * @return the WriteFile if everything went well
   */
  std::auto_ptr<castor::tape::tapeFile::WriteFile> openWriteFile(
  castor::tape::tapeFile::WriteSession & session,castor::log::LogContext& lc);

  /**
   * All we need to know about the file we are migrating
   */
  std::auto_ptr<tapegateway::FileToMigrateStruct> m_fileToMigrate;
  
  /**
   * reference to the memory manager in use   
   */
  MigrationMemoryManager & m_memManager;
  
  /**
   * The fifo containing the memory blocks holding data to be written to tape
   */
  DataFifo m_fifo;
  
  /**
   * Mutex forcing serial access to the fifo
   */
  castor::tape::threading::Mutex m_producerProtection;
  
  /**
   * The number of memory blocks to be used
   */
  int m_blockCount;
  
  castor::tape::threading::AtomicFlag& m_errorFlag;
};

}}}}


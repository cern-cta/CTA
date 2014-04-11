/******************************************************************************
 *                      DiskReadFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/DiskReadTaskInterface.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
class DiskReadTask :public DiskReadTaskInterface {
public:
  DiskReadTask(DataConsumer & destination, tape::tapegateway::FileToMigrateStruct* file):
       m_nextTask(destination),m_migratedFile(file) {}

  virtual void execute(log::LogContext& lc) {
    size_t blockId=0;
    size_t migratingFileSize=m_migratedFile->fileSize();
    try{
      tape::diskFile::ReadFile sourceFile(m_migratedFile->path());
      while(migratingFileSize>0){
        blockId++;
        MemBlock * mb = m_nextTask.getFreeBlock();
        mb->m_fileid = m_migratedFile->fileid();
        //mb->m_fileBlock = blockId;
        
        mb->m_payload.read(sourceFile);
        migratingFileSize-= mb->m_payload.size();
        
        //we either read at full capacity (ie size=capacity) or if there different,
        //it should be the end => migratingFileSize should be 0. If it not, error
        if(mb->m_payload.size() != mb->m_payload.totalCapacity() && migratingFileSize>0){
          throw castor::tape::Exception("Error while reading a file. Did not read at full capacity but the file is not fully read");
        }
        m_nextTask.pushDataBlock(mb);
      } //end of while(migratingFileSize>0)
    }
    catch(const castor::tape::Exception& e){
      //we have to pump block anyway, mark them failed and then pass them to TapeWrite
      //Otherwise they would be stuck into TapeWriteTask free block fifo

      using log::LogContext;
      using log::Param;
      
      LogContext::ScopedParam sp(lc, Param("blockID",blockId));
      lc.log(LOG_ERR,e.getMessageValue());
      while(migratingFileSize>0) {
        MemBlock * mb = m_nextTask.getFreeBlock();
        mb->m_failed=true;
        m_nextTask.pushDataBlock(mb);
      } //end of while
    } //end of catch
  }
private:
  //TW ; tape write
  DataConsumer & m_nextTask;
  std::auto_ptr<tape::tapegateway::FileToMigrateStruct> m_migratedFile;
};

}}}}


/******************************************************************************
 *                      MigrationTaskInjector.cpp
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

#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"

namespace{

  castor::tape::tapegateway::FileToMigrateStruct* removeOwningList(castor::tape::tapegateway::FileToMigrateStruct* ptr){
    ptr->setFilesToMigrateList(0);
    return ptr;
  }
}

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {


  MigrationTaskInjector::MigrationTaskInjector(MemoryManager & mm, 
          DiskThreadPoolInterface<DiskReadTaskInterface> & diskReader,
          TapeSingleThreadInterface<TapeWriteTask> & tapeWriter,client::ClientInterface& client,
          castor::log::LogContext lc):
          m_thread(*this),m_memManager(mm),m_tapeWriter(tapeWriter),
          m_diskReader(diskReader),m_client(client)
  {
    
  }
  
 
  /**
   * Create all the tape-read and write-disk tasks for set of files to retrieve
   * @param jobs
   */
  void MigrationTaskInjector::injectBulkMigrations(const std::vector<tapegateway::FileToMigrateStruct*>& jobs){
    const u_signed64 blockCapacity = m_memManager->blockCapacity();
    for(const std::vector<tapegateway::FileToMigrateStruct*>::const_iterator it= jobs.begin();it!=jobs.end();++it){
      const u_signed64 fileSize = (*it)->fileSize();
      
      const u_signed64 neededBlock = fileSize/blockCapacity + ((fileSize%blockCapacity==0) ? 0 : 1); 
      
      TapeWriteTask *twt = new TapeWriteTask(,neededBlock,mm);
      DiskReadTask *drt = new DiskReadTask(*twt,removeOwningList((*it)->clone()));
    }
  }
  
  /**
   * Wait for the inner thread to finish
   */
  void MigrationTaskInjector::waitThreads(){
    m_thread.wait();
  }
  
  /**
   * Start the inner thread 
   */
  void MigrationTaskInjector::startThreads(){
    m_thread.start();
  }

 

    MigrationTaskInjector::WorkerThread::WorkerThread(MigrationTaskInjector & rji): _this(rji) {}
    void MigrationTaskInjector::WorkerThread::run(){}


} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

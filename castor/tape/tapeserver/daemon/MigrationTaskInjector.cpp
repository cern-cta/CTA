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
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "log.h"

namespace{

  castor::tape::tapegateway::FileToMigrateStruct* removeOwningList(castor::tape::tapegateway::FileToMigrateStruct* ptr){
    ptr->setFilesToMigrateList(0);
    return ptr;
  }
}

using castor::log::LogContext;
using castor::log::Param;

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {


  MigrationTaskInjector::MigrationTaskInjector(MemoryManager & mm, 
          DiskThreadPoolInterface<DiskReadTaskInterface> & diskReader,
          TapeSingleThreadInterface<TapeWriteTask> & tapeWriter,client::ClientInterface& client,
          castor::log::LogContext lc):
          m_thread(*this),m_memManager(mm),m_tapeWriter(tapeWriter),
          m_diskReader(diskReader),m_client(client),m_lc(lc)
  {
    
  }
  
 
  /**
   * Create all the tape-read and write-disk tasks for set of files to retrieve
   * @param jobs to transform into tasks
   */
  void MigrationTaskInjector::injectBulkMigrations(const std::vector<tapegateway::FileToMigrateStruct*>& jobs){

    const u_signed64 blockCapacity = m_memManager.blockCapacity();
    for(std::vector<tapegateway::FileToMigrateStruct*>::const_iterator it= jobs.begin();it!=jobs.end();++it){
      const u_signed64 fileSize = (*it)->fileSize();
      LogContext::ScopedParam sp[]={
        LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->fileid())),
        LogContext::ScopedParam(m_lc, Param("NSFILESEQNUMBER", (*it)->fseq())),
        LogContext::ScopedParam(m_lc, Param("NSFILENSHOST", (*it)->nshost())),
        LogContext::ScopedParam(m_lc, Param("NSFILEPATH", (*it)->path()))
      };
      tape::utils::suppresUnusedVariable(sp);
      m_lc.log(LOG_INFO, "Logged file to migrate");
      
      const u_signed64 neededBlock = fileSize/blockCapacity + ((fileSize%blockCapacity==0) ? 0 : 1); 
      
      TapeWriteTask *twt = new TapeWriteTask(neededBlock,removeOwningList((*it)->clone()),m_memManager);
      DiskReadTask *drt = new DiskReadTask(*twt,removeOwningList((*it)->clone()),neededBlock);
      
      m_tapeWriter.push(twt);
      m_diskReader.push(drt);
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
  
  void MigrationTaskInjector::requestInjection(int maxFiles, int byteSizeThreshold, bool lastCall) {
    //@TODO where shall we  acquire the lock ? There of just before the push ?
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_queue.push(Request(maxFiles, byteSizeThreshold, lastCall));
  }
 
//------------------------------------------------------------------------------  
  void MigrationTaskInjector::WorkerThread::run(){
    _this.m_lc.pushOrReplace(Param("thread", "MigrationTaskInjector"));
    _this.m_lc.log(LOG_DEBUG, "Starting MigrationTaskInjector thread");
    while(1){
      Request req = _this.m_queue.pop();
      client::ClientProxy::RequestReport reqReport;
      std::auto_ptr<tapegateway::FilesToMigrateList> filesToMigrateList(_this.m_client.getFilesToMigrate(req.nbMaxFiles, req.byteSizeThreshold,reqReport));

      if(NULL==filesToMigrateList.get()){
        if (req.lastCall) {
          _this.m_lc.log(LOG_INFO,"No more file to migrate: triggering the end of session.\n");
          _this.m_tapeWriter.finish();
          _this.m_diskReader.finish();
          break;
        } else {
          _this.m_lc.log(LOG_INFO,"In MigrationTaskInjector::WorkerThread::run(): got empty list, but not last call");
        }
      } else {
        _this.injectBulkMigrations(filesToMigrateList->filesToMigrate());
      }
    }
    _this.m_lc.log(LOG_DEBUG, "Finishing MigrationTaskInjector thread");
  }


} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

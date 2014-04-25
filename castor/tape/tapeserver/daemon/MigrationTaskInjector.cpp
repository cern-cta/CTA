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
      LogContext::ScopedParam(m_lc, Param("NSHOSTNAME", (*it)->nshost())),
      LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->fileid())),
      LogContext::ScopedParam(m_lc, Param("fSeq", (*it)->fseq())),
      LogContext::ScopedParam(m_lc, Param("path", (*it)->path()))
      };
      tape::utils::suppresUnusedVariable(sp);
      
      
      const u_signed64 neededBlock = howManyBlocksNeeded(fileSize,blockCapacity);
      
      std::auto_ptr<TapeWriteTask> twt(
        new TapeWriteTask(neededBlock,removeOwningList((*it)->clone()),m_memManager,m_errorFlag)
      );
      std::auto_ptr<DiskReadTask> drt(
        new DiskReadTask(*twt,removeOwningList((*it)->clone()),neededBlock,m_errorFlag)
      );
      
      m_tapeWriter.push(twt.release());
      m_diskReader.push(drt.release());
      m_lc.log(LOG_INFO, "Logged file to migrate");
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
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_queue.push(Request(maxFiles, byteSizeThreshold, lastCall));
  }
 
//------------------------------------------------------------------------------  
  void MigrationTaskInjector::WorkerThread::run(){
    m_parent.m_lc.pushOrReplace(Param("thread", "MigrationTaskInjector"));
    m_parent.m_lc.log(LOG_DEBUG, "Starting MigrationTaskInjector thread");
    try{
      while(1){
        if(m_parent.m_errorFlag){
          throw castor::tape::exceptions::ErrorFlag();
        }
        Request req = m_parent.m_queue.pop();
        client::ClientProxy::RequestReport reqReport;
        std::auto_ptr<tapegateway::FilesToMigrateList> filesToMigrateList(m_parent.m_client.getFilesToMigrate(req.nbMaxFiles, req.byteSizeThreshold,reqReport));
        
        if(NULL==filesToMigrateList.get()){
          if (req.lastCall) {
            m_parent.m_lc.log(LOG_INFO,"No more file to migrate: triggering the end of session.\n");
            m_parent.m_tapeWriter.finish();
            m_parent.m_diskReader.finish();
            break;
          } else {
            m_parent.m_lc.log(LOG_INFO,"In MigrationTaskInjector::WorkerThread::run(): got empty list, but not last call");
          }
        } else {
          m_parent.injectBulkMigrations(filesToMigrateList->filesToMigrate());
        }
      } //end of while(1)
    }//end of try
    catch(const castor::tape::exceptions::ErrorFlag&){
      //discard all the tasks !!
      while(m_parent.m_queue.size()>0){
        Request req = m_parent.m_queue.pop();
      }
    }
    m_parent.m_lc.log(LOG_DEBUG, "Finishing MigrationTaskInjector thread");
  }


} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

/******************************************************************************
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
  /*
   * function to set a NULL the owning FilesToMigrateList  of a FileToMigrateStruct
   * Indeed, a clone of a structure will only do a shallow copy (sic).
   * Otherwise at the second destruction the object will try to remove itself 
   * from the owning list and then boom !
   * @param ptr a pointer to an object to change
   * @return the parameter ptr 
   */
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

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
  MigrationTaskInjector::MigrationTaskInjector(MigrationMemoryManager & mm, 
          DiskReadThreadPool & diskReader,
          TapeSingleThreadInterface<TapeWriteTask> & tapeWriter,client::ClientInterface& client,
           uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc):
          m_thread(*this),m_memManager(mm),m_tapeWriter(tapeWriter),
          m_diskReader(diskReader),m_client(client),m_lc(lc),
          m_maxFiles(maxFiles),  m_maxBytes(byteSizeThreshold)
  {
    
  }
  
 
//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
  void MigrationTaskInjector::injectBulkMigrations(
  const std::vector<tapegateway::FileToMigrateStruct*>& jobs){

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
      m_lc.log(LOG_INFO, "Created tasks for migrating a file");
    }
    LogContext::ScopedParam(m_lc, Param("numbnerOfFiles", jobs.size()));
    m_lc.log(LOG_INFO, "Finished creating tasks for migrating");
  }
  
//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
  void MigrationTaskInjector::waitThreads(){
    m_thread.wait();
  }
  
//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
  void MigrationTaskInjector::startThreads(){
    m_thread.start();
  }
  
//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
  void MigrationTaskInjector::requestInjection( bool lastCall) {
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    if(!m_errorFlag) {
      m_queue.push(Request(m_maxFiles, m_maxBytes, lastCall));
    }
  }
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------ 
  bool MigrationTaskInjector::synchronousInjection() {
    client::ClientProxy::RequestReport reqReport;
    std::auto_ptr<tapegateway::FilesToMigrateList>
      filesToMigrateList;
    try {
      filesToMigrateList.reset(m_client.getFilesToMigrate(m_maxFiles, 
        m_maxBytes,reqReport));
    } catch (castor::exception::Exception & ex) {
      castor::log::ScopedParamContainer scoped(m_lc);
      scoped.add("transactionId", reqReport.transactionId)
            .add("byteSizeThreshold",m_maxBytes)
            .add("maxFiles", m_maxFiles)
            .add("message", ex.getMessageValue());
      m_lc.log(LOG_ERR, "Failed to getFiledToRecall.");
      return false;
    }
    castor::log::ScopedParamContainer scoped(m_lc); 
    scoped.add("sendRecvDuration", reqReport.sendRecvDuration)
          .add("byteSizeThreshold",m_maxBytes)
          .add("transactionId", reqReport.transactionId)
          .add("maxFiles", m_maxFiles);
    if(NULL == filesToMigrateList.get()) {
      m_lc.log(LOG_ERR, "No files to migrate: empty mount");
      return false;
    } else {
      std::vector<tapegateway::FileToMigrateStruct*>& jobs=filesToMigrateList->filesToMigrate();
      m_firstFseqToWrite = jobs.front()->fseq();
      injectBulkMigrations(jobs);
      return true;
    }
  }
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------ 
  void MigrationTaskInjector::finish(){
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_queue.push(Request());
  }
//------------------------------------------------------------------------------
//signalEndDataMovement
//------------------------------------------------------------------------------   
  void MigrationTaskInjector::signalEndDataMovement(){        
    //first send the end signal to the threads
    m_tapeWriter.finish();
    m_diskReader.finish();
    m_memManager.finish();
  }
//------------------------------------------------------------------------------
//deleteAllTasks
//------------------------------------------------------------------------------     
  void MigrationTaskInjector::deleteAllTasks(){
    //discard all the tasks !!
    while(m_queue.size()>0){
      m_queue.pop();
    }
  }
  //------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
  void MigrationTaskInjector::WorkerThread::run(){
    m_parent.m_lc.pushOrReplace(Param("thread", "MigrationTaskInjector"));
    m_parent.m_lc.log(LOG_INFO, "Starting MigrationTaskInjector thread");
    try{
      while(1){
        if(m_parent.m_errorFlag){
          throw castor::tape::exceptions::ErrorFlag();
        }
        Request req = m_parent.m_queue.pop();
        client::ClientProxy::RequestReport reqReport;
        std::auto_ptr<tapegateway::FilesToMigrateList> filesToMigrateList(
          m_parent.m_client.getFilesToMigrate(req.nbMaxFiles, req.byteSizeThreshold,reqReport)
        );

        if(NULL==filesToMigrateList.get()){
          if (req.lastCall) {
            m_parent.m_lc.log(LOG_INFO,"No more file to migrate: triggering the end of session.\n");
            m_parent.signalEndDataMovement();
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
      //we end up there because a task screw up somewhere 
      m_parent.m_lc.log(LOG_ERR,"In MigrationTaskInjector::WorkerThread::run(): a task screw up, "
      "finishing and discarding all tasks ");
      
      m_parent.signalEndDataMovement();
      m_parent.deleteAllTasks();
    } 
    catch(const castor::exception::Exception& ex){
      //we end up there because we could not talk to the client
      
      log::ScopedParamContainer container( m_parent.m_lc);
      container.add("exception code",ex.code())
               .add("exception message",ex.getMessageValue());
      m_parent.m_lc.logBacktrace(LOG_ERR,ex.backtrace());
      m_parent.m_lc.log(LOG_ERR,"In MigrationTaskInjector::WorkerThread::run(): "
      "could not retrieve a list of file to migrate. End of session");
      
      m_parent.signalEndDataMovement();
      m_parent.deleteAllTasks();
    } 
    //-------------
    m_parent.m_lc.log(LOG_INFO, "Finishing MigrationTaskInjector thread");
    /* We want to finish at the first lastCall we encounter.
     * But even after sending finish() to m_diskWriter and to m_tapeReader,
     * m_diskWriter might still want some more task (the threshold could be crossed),
     * so we discard everything that might still be in the queue
     */
    bool stillReading =true;
    while(stillReading) {
      Request req = m_parent.m_queue.pop();
      if (req.end) stillReading = false;
      LogContext::ScopedParam sp(m_parent.m_lc, Param("lastCall", req.lastCall));
      m_parent.m_lc.log(LOG_INFO,"In MigrationTaskInjector::WorkerThread::run(): popping extra request");
    }
  }

  uint64_t MigrationTaskInjector::firstFseqToWrite() const {
    return m_firstFseqToWrite;
  }
} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

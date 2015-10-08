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
#include "castor/tape/tapeserver/daemon/ErrorFlag.hpp"

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
          TapeSingleThreadInterface<TapeWriteTask> & tapeWriter,cta::ArchiveMount &archiveMount,
           uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc):
          m_thread(*this),m_memManager(mm),m_tapeWriter(tapeWriter),
          m_diskReader(diskReader),m_archiveMount(archiveMount),m_lc(lc),
          m_maxFiles(maxFiles),  m_maxBytes(byteSizeThreshold)
  {
    
  }
  
 
//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
  void MigrationTaskInjector::injectBulkMigrations(const std::vector<cta::ArchiveJob *>& jobs){

    const u_signed64 blockCapacity = m_memManager.blockCapacity();
    for(auto it= jobs.begin();it!=jobs.end();++it){
      const u_signed64 fileSize = (*it)->archiveFile.size;
      LogContext::ScopedParam sp[]={
      LogContext::ScopedParam(m_lc, Param("NSHOSTNAME", (*it)->archiveFile.nsHostName)),
      LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->archiveFile.fileId)),
      LogContext::ScopedParam(m_lc, Param("fSeq", (*it)->nameServerTapeFile.tapeFileLocation.fSeq)),
      LogContext::ScopedParam(m_lc, Param("path", (*it)->remotePathAndStatus.path.getRaw()))
      };
      tape::utils::suppresUnusedVariable(sp);      
      
      const u_signed64 neededBlock = howManyBlocksNeeded(fileSize,blockCapacity);
      
      std::unique_ptr<TapeWriteTask> twt(new TapeWriteTask(neededBlock, *it, m_memManager, m_errorFlag));
      std::unique_ptr<DiskReadTask> drt(new DiskReadTask(*twt, *it, neededBlock, m_errorFlag));
      
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
    castor::server::MutexLocker ml(&m_producerProtection);
    m_queue.push(Request(m_maxFiles, m_maxBytes, lastCall));

  }
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------ 
  bool MigrationTaskInjector::synchronousInjection() {
    std::vector<cta::ArchiveJob *> jobs;
    try {
      uint64_t files=0;
      uint64_t bytes=0;
      while(files<=m_maxFiles && bytes<=m_maxBytes) {
        std::unique_ptr<cta::ArchiveJob> job=m_archiveMount.getNextJob();
        if(!job.get()) break;
        files++;
        bytes+=job->archiveFile.size;
        jobs.push_back(job.release());
      }
    } catch (castor::exception::Exception & ex) {
      castor::log::ScopedParamContainer scoped(m_lc);
      scoped.add("transactionId", m_archiveMount.getMountTransactionId())
            .add("byteSizeThreshold",m_maxBytes)
            .add("maxFiles", m_maxFiles)
            .add("message", ex.getMessageValue());
      m_lc.log(LOG_ERR, "Failed to getFilesToMigrate");
      return false;
    }
    castor::log::ScopedParamContainer scoped(m_lc); 
    scoped.add("byteSizeThreshold",m_maxBytes)
          .add("maxFiles", m_maxFiles);
    if(jobs.empty()) {
      m_lc.log(LOG_ERR, "No files to migrate: empty mount");
      return false;
    } else {
      m_firstFseqToWrite = jobs.front()->nameServerTapeFile.tapeFileLocation.fSeq;
      injectBulkMigrations(jobs);
      return true;
    }
  }
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------ 
  void MigrationTaskInjector::finish(){
    castor::server::MutexLocker ml(&m_producerProtection);
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
//WorkerThread::run
//------------------------------------------------------------------------------
  void MigrationTaskInjector::WorkerThread::run(){
    m_parent.m_lc.pushOrReplace(Param("thread", "MigrationTaskInjector"));
    m_parent.m_lc.log(LOG_INFO, "Starting MigrationTaskInjector thread");
    try{
      while(1){
        if(m_parent.m_errorFlag){
          throw castor::tape::tapeserver::daemon::ErrorFlag();
        }
        Request req = m_parent.m_queue.pop();
        std::vector<cta::ArchiveJob *> jobs;
        
        uint64_t files=0;
        uint64_t bytes=0;
        while(files<=req.filesRequested && bytes<=req.bytesRequested) {
          std::unique_ptr<cta::ArchiveJob> job=m_parent.m_archiveMount.getNextJob();
          if(!job.get()) break;
          files++;
          bytes+=job->archiveFile.size;
          jobs.push_back(job.release());
        }

        if(jobs.empty()){
          if (req.lastCall) {
            m_parent.m_lc.log(LOG_INFO,"No more file to migrate: triggering the end of session.");
            m_parent.signalEndDataMovement();
            break;
          } else {
            m_parent.m_lc.log(LOG_INFO,"In MigrationTaskInjector::WorkerThread::run(): got empty list, but not last call");
          }
        } else {
          
          // Inject the tasks
          m_parent.injectBulkMigrations(jobs);
          // Decide on continuation
          if(files < req.filesRequested / 2 && bytes < req.bytesRequested) {
            // The client starts to dribble files at a low rate. Better finish
            // the session now, so we get a clean batch on a later mount.
            log::ScopedParamContainer params(m_parent.m_lc);
            params.add("filesRequested", req.filesRequested)
                  .add("bytesRequested", req.bytesRequested)
                  .add("filesReceived", files)
                  .add("bytesReceived", bytes);
            m_parent.m_lc.log(LOG_INFO, "Got less than half the requested work to do: triggering the end of session.");
            m_parent.signalEndDataMovement();
            break;
          }
        }
      } //end of while(1)
    }//end of try
    catch(const castor::tape::tapeserver::daemon::ErrorFlag&){
      //we end up there because a task screw up somewhere 
      m_parent.m_lc.log(LOG_INFO,"In MigrationTaskInjector::WorkerThread::run(): a task failed, "
      "indicating finish of run");
      
      m_parent.signalEndDataMovement();
    } 
    catch(const castor::exception::Exception& ex){
      //we end up there because we could not talk to the client
      
      log::ScopedParamContainer container( m_parent.m_lc);
      container.add("exception code",ex.code())
               .add("exception message",ex.getMessageValue());
      m_parent.m_lc.logBacktrace(LOG_ERR,ex.backtrace());
      m_parent.m_lc.log(LOG_ERR,"In MigrationTaskInjector::WorkerThread::run(): "
      "could not retrieve a list of file to migrate, indicating finish of run");
      
      m_parent.signalEndDataMovement();
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

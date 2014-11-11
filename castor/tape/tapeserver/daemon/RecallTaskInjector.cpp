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
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "log.h"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include <stdint.h>

using castor::log::LogContext;
using castor::log::Param;

namespace{
  /**
   *  function to set a NULL the owning FilesToMigrateList  of a FileToMigrateStruct
   *   Indeed, a clone of a structure will only do a shallow copy (sic).
   *   Otherwise at the second destruction the object will try to remove itself 
   *   from the owning list and then boom !
   * @param ptr a pointer to an object to change
   * @return the parameter ptr 
   */
  castor::tape::tapegateway::FileToRecallStruct* removeOwningList(castor::tape::tapegateway::FileToRecallStruct* ptr){
    ptr->setFilesToRecallList(NULL);
    return ptr;
  }
}

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {
  
RecallTaskInjector::RecallTaskInjector(RecallMemoryManager & mm, 
        TapeSingleThreadInterface<TapeReadTask> & tapeReader,
        DiskWriteThreadPool & diskWriter,
        client::ClientInterface& client,
        uint64_t maxFiles, uint64_t byteSizeThreshold,castor::log::LogContext lc) : 
        m_thread(*this),m_memManager(mm),
        m_tapeReader(tapeReader),m_diskWriter(diskWriter),
        m_client(client),m_lc(lc),m_maxFiles(maxFiles),m_maxBytes(byteSizeThreshold),
        m_clientType(tapegateway::READ_TP)
{}
//------------------------------------------------------------------------------
//destructor
//------------------------------------------------------------------------------
RecallTaskInjector::~RecallTaskInjector(){
}
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------

void RecallTaskInjector::finish(){
  castor::server::MutexLocker ml(&m_producerProtection);
  m_queue.push(Request());
}
//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
void RecallTaskInjector::requestInjection(bool lastCall) {
  //@TODO where shall we  acquire the lock ? There of just before the push ?
  castor::server::MutexLocker ml(&m_producerProtection);
  m_queue.push(Request(m_maxFiles, m_maxBytes, lastCall));
}
//------------------------------------------------------------------------------
//waitThreads
//------------------------------------------------------------------------------
void RecallTaskInjector::waitThreads() {
  m_thread.wait();
}
//------------------------------------------------------------------------------
//startThreads
//------------------------------------------------------------------------------
void RecallTaskInjector::startThreads() {
  m_thread.start();
}
//------------------------------------------------------------------------------
//injectBulkRecalls
//------------------------------------------------------------------------------
void RecallTaskInjector::injectBulkRecalls(const std::vector<castor::tape::tapegateway::FileToRecallStruct*>& jobs) {
  for (std::vector<tapegateway::FileToRecallStruct*>::const_iterator it = jobs.begin(); it != jobs.end(); ++it) {
    
    // Workaround for bug CASTOR-4829: tapegateway: should request positioning by blockid for recalls instead of fseq
    // When the client is a tape gateway, we should *always* position by block id.
    if (tapegateway::TAPE_GATEWAY == m_clientType) {
      (*it)->setPositionCommandCode(tapegateway::TPPOSIT_BLKID);
    }

    LogContext::ScopedParam sp[]={
      LogContext::ScopedParam(m_lc, Param("NSHOSTNAME", (*it)->nshost())),
      LogContext::ScopedParam(m_lc, Param("NSFILEID", (*it)->fileid())),
      LogContext::ScopedParam(m_lc, Param("fSeq", (*it)->fseq())),
      LogContext::ScopedParam(m_lc, Param("blockID", blockID(**it))),
      LogContext::ScopedParam(m_lc, Param("path", (*it)->path()))
    };
    tape::utils::suppresUnusedVariable(sp);
    
    m_lc.log(LOG_INFO, "Recall task created");
    
    DiskWriteTask * dwt = 
      new DiskWriteTask(
        removeOwningList(dynamic_cast<tape::tapegateway::FileToRecallStruct*>((*it)->clone())), 
        m_memManager);
    TapeReadTask * trt = 
      new TapeReadTask(
        removeOwningList(
          dynamic_cast<tape::tapegateway::FileToRecallStruct*>((*it)->clone())), 
          *dwt,
          m_memManager);
    
    m_diskWriter.push(dwt);
    m_tapeReader.push(trt);
  }
  LogContext::ScopedParam sp03(m_lc, Param("nbFile", jobs.size()));
  m_lc.log(LOG_INFO, "Finished processing batch of recall tasks from client");
}
//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool RecallTaskInjector::synchronousInjection()
{
  client::ClientProxy::RequestReport reqReport;  

  std::auto_ptr<castor::tape::tapegateway::FilesToRecallList> 
    filesToRecallList;
  try {
    filesToRecallList.reset(m_client.getFilesToRecall(m_maxFiles,m_maxBytes,reqReport));
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
  if(NULL==filesToRecallList.get()) { 
    m_lc.log(LOG_ERR, "No files to recall: empty mount");
    return false;
  }
  else {
    std::vector<tapegateway::FileToRecallStruct*>& jobs= filesToRecallList->filesToRecall();
    injectBulkRecalls(jobs);
    return true;
  }
}
//------------------------------------------------------------------------------
//signalEndDataMovement
//------------------------------------------------------------------------------
  void RecallTaskInjector::signalEndDataMovement(){        
    //first send the end signal to the threads
    m_tapeReader.finish();
    m_diskWriter.finish();
  }
//------------------------------------------------------------------------------
//deleteAllTasks
//------------------------------------------------------------------------------
  void RecallTaskInjector::deleteAllTasks(){
    //discard all the tasks !!
    while(m_queue.size()>0){
      m_queue.pop();
    }
  }
  
//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void RecallTaskInjector::WorkerThread::run()
{
  using castor::log::LogContext;
  m_parent.m_lc.pushOrReplace(Param("thread", "recallTaskInjector"));
  m_parent.m_lc.log(LOG_DEBUG, "Starting RecallTaskInjector thread");
  
  try{
    while (1) {
      Request req = m_parent.m_queue.pop();
      if (req.end) {
        m_parent.m_lc.log(LOG_INFO,"Received a end notification from tape thread: triggering the end of session.");
        m_parent.signalEndDataMovement();
        break;
      }
      m_parent.m_lc.log(LOG_DEBUG,"RecallJobInjector:run: about to call client interface");
      client::ClientProxy::RequestReport reqReport;
      std::auto_ptr<tapegateway::FilesToRecallList> filesToRecallList(m_parent.m_client.getFilesToRecall(req.nbMaxFiles, req.byteSizeThreshold,reqReport));
      
      LogContext::ScopedParam sp01(m_parent.m_lc, Param("transactionId", reqReport.transactionId));
      LogContext::ScopedParam sp02(m_parent.m_lc, Param("connectDuration", reqReport.connectDuration));
      LogContext::ScopedParam sp03(m_parent.m_lc, Param("sendRecvDuration", reqReport.sendRecvDuration));
      
      if (NULL == filesToRecallList.get()) {
        if (req.lastCall) {
          m_parent.m_lc.log(LOG_INFO,"No more file to recall: triggering the end of session.");
          m_parent.signalEndDataMovement();
          break;
        } else {
          m_parent.m_lc.log(LOG_DEBUG,"In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.");
        }
      } else {
        std::vector<tapegateway::FileToRecallStruct*>& jobs= filesToRecallList->filesToRecall();
        m_parent.injectBulkRecalls(jobs);
      }
    } // end of while(1)
  } //end of try
  catch(const castor::exception::Exception& ex){
      //we end up there because we could not talk to the client
      log::ScopedParamContainer container( m_parent.m_lc);
      container.add("exception code",ex.code())
               .add("exception message",ex.getMessageValue());
      m_parent.m_lc.logBacktrace(LOG_ERR,ex.backtrace());
      m_parent.m_lc.log(LOG_ERR,"In RecallJobInjector::WorkerThread::run(): "
      "could not retrieve a list of file to recall. End of session");
      
      m_parent.signalEndDataMovement();
      m_parent.deleteAllTasks();
    }
  //-------------
  m_parent.m_lc.log(LOG_DEBUG, "Finishing RecallTaskInjector thread");
  /* We want to finish at the first lastCall we encounter.
   * But even after sending finish() to m_diskWriter and to m_tapeReader,
   * m_diskWriter might still want some more task (the threshold could be crossed),
   * so we discard everything that might still be in the queue
   */
  if(m_parent.m_queue.size()>0) {
    bool stillReading =true;
    while(stillReading) {
      Request req = m_parent.m_queue.pop();
      if (req.end){
        stillReading = false;
      }
      LogContext::ScopedParam sp(m_parent.m_lc, Param("lastCall", req.lastCall));
      m_parent.m_lc.log(LOG_DEBUG,"In RecallJobInjector::WorkerThread::run(): popping extra request");
    }
  }
}

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

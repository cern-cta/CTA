/******************************************************************************
 *                      DiskReadThreadPool.cpp
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

#include "castor/tape/tapeserver/daemon/DiskReadThreadPool.hpp"
#include <memory>
#include <sstream>
#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"
#include "log.h"
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
  DiskReadThreadPool::DiskReadThreadPool(int nbThread, unsigned int maxFilesReq,unsigned int maxBytesReq, 
          castor::log::LogContext lc) : m_lc(lc),m_maxFilesReq(maxFilesReq),m_maxBytesReq(maxBytesReq),m_nbActiveThread(0){
    for(int i=0; i<nbThread; i++) {
      DiskReadWorkerThread * thr = new DiskReadWorkerThread(*this);
      m_threads.push_back(thr);
      m_lc.pushOrReplace(log::Param("threadID",i));
      m_lc.log(LOG_INFO, "DiskReadWorkerThread created");
    }
  }
  DiskReadThreadPool::~DiskReadThreadPool() { 
    while (m_threads.size()) {
      delete m_threads.back();
      m_threads.pop_back();
    }
    m_lc.log(LOG_INFO, "All the DiskReadWorkerThreads have been destroyed");
  }
  void DiskReadThreadPool::startThreads() {
    for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->startThreads();
    }
    m_lc.log(LOG_INFO, "All the DiskReadWorkerThreads are started");
  }
  void DiskReadThreadPool::waitThreads() {
    for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->waitThreads();
    }
  }
  void DiskReadThreadPool::push(DiskReadTaskInterface *t) { 
    m_tasks.push(t); 
    m_lc.log(LOG_INFO, "Push a task into the DiskReadThreadPool");
  }
  void DiskReadThreadPool::finish() {
    /* Insert one endOfSession per thread */
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(NULL);
    }
  }
  DiskReadTaskInterface* DiskReadThreadPool::popAndRequestMore(){
    castor::tape::threading::BlockingQueue<DiskReadTaskInterface*>::valueRemainingPair 
    vrp = m_tasks.popGetSize();
    log::LogContext::ScopedParam sp(m_lc, log::Param("m_maxFilesReq", m_maxFilesReq));
    log::LogContext::ScopedParam sp0(m_lc, log::Param("m_maxBytesReq", m_maxBytesReq));

    if(0==vrp.remaining){
      m_injector->requestInjection(m_maxFilesReq, m_maxBytesReq,true);
      m_lc.log(LOG_DEBUG, "Requested injection from MigrationTaskInjector (with last call)");
    }else if(vrp.remaining + 1 ==  m_maxFilesReq/2){
      m_injector->requestInjection(m_maxFilesReq, m_maxBytesReq,false);
      m_lc.log(LOG_DEBUG, "Requested injection from MigrationTaskInjector (without last call)");
    }
    return vrp.value;
  }
  void DiskReadThreadPool::DiskReadWorkerThread::run() {
    m_lc.pushOrReplace(log::Param("threadID",m_threadID));
    m_lc.log(LOG_DEBUG, "DiskReadWorkerThread Running");
    std::auto_ptr<DiskReadTaskInterface> task;
    while(1) {
      task.reset( m_parent.popAndRequestMore());
      if (NULL!=task.get()) {
        task->execute(m_lc);
      }
      else {
        break;
      }
    } //end of while(1)
    // We now acknowledge to the task injector that read reached the end. There
    // will hence be no more requests for more. (last thread turns off the light)
    if (0 == --m_parent.m_nbActiveThread) {
      m_parent.m_injector->finish();
      m_lc.log(LOG_INFO, "Signaled to task injector the end of disk read threads");
    }
    m_lc.log(LOG_INFO, "Finishing of DiskReadWorkerThread");
  }
  
}}}}


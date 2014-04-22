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
  
  DiskReadThreadPool::DiskReadThreadPool(int nbThread, int m_maxFilesReq,int m_maxBytesReq, 
          castor::log::LogContext lc) : m_lc(lc){
    for(int i=0; i<nbThread; i++) {
      DiskReadWorkerThread * thr = new DiskReadWorkerThread(*this);
      m_threads.push_back(thr);
    }
  }
  DiskReadThreadPool::~DiskReadThreadPool() { 
    while (m_threads.size()) {
      delete m_threads.back();
      m_threads.pop_back();
    }
  }
  void DiskReadThreadPool::startThreads() {
    for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->startThreads();
    }
  }
  void DiskReadThreadPool::waitThreads() {
    for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->waitThreads();
    }
  }
  void DiskReadThreadPool::push(DiskReadTaskInterface *t) { 
    m_tasks.push(t); 
  }
  void DiskReadThreadPool::finish() {
    /* Insert one endOfSession per thread */
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(NULL);
    }
  }
  DiskReadTaskInterface* DiskReadThreadPool::popAndRequestMore(){
    DiskReadTaskInterface* ret=m_tasks.pop();
    castor::tape::threading::TryMutexLocker locker(&m_loopBackMutex);
    if(locker)
    {
      const int remainningTasks = m_tasks.size();
      if(1==remainningTasks){
        m_injector->requestInjection(m_maxFilesReq, m_maxBytesReq,true);
      }else if(remainningTasks <= m_maxFilesReq/2){
        m_injector->requestInjection(m_maxFilesReq, m_maxBytesReq,false);
      }
    }
    return ret;
  }
  void DiskReadThreadPool::DiskReadWorkerThread::run() {
    m_lc.pushOrReplace(log::Param("thread", "DiskRead"));
    m_lc.log(LOG_DEBUG, "Starting DiskReadWorkerThread");
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
    // will hence be no more requests for more.
    m_parent.m_injector->finish();
    m_lc.log(LOG_DEBUG, "Finishing of DiskReadWorkerThread");
  }
  
  tape::threading::AtomicCounter<int> DiskReadThreadPool::DiskReadWorkerThread::m_nbActiveThread(0);
}}}}


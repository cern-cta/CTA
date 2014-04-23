/******************************************************************************
 *                      DiskReadThreadPool.hpp
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
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/daemon/DiskThreadPoolInterface.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/log/LogContext.hpp"
#include <vector>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  class MigrationTaskInjector;
class DiskReadThreadPool : public DiskThreadPoolInterface<DiskReadTaskInterface> {
public:
  /**
   * @param nbThread Number of thread for reading files 
   * @param maxFilesReq maximal number of files we might require 
   * within a single request to the task injectore
   * @param maxBytesReq maximal number of bytes we might require
   *  within a single request a single request to the task injectore
   * @param lc log context fpr logging purpose
   */
  DiskReadThreadPool(int nbThread, unsigned int maxFilesReq,unsigned int maxBytesReq, 
          castor::log::LogContext lc);
  
  ~DiskReadThreadPool();
  
  void startThreads();
  void waitThreads();
  
  virtual void push(DiskReadTaskInterface *t);
  void finish();
  void setTaskInjector(MigrationTaskInjector* injector){
      m_injector = injector;
  }
private:
  /** Get the next task to execute and if there is not enough tasks in queue,
   * it will ask the TaskInjector to get more job 
   * @return the next task to execute
   */
  DiskReadTaskInterface* popAndRequestMore();
  
  class DiskReadWorkerThread: private castor::tape::threading::Thread {
  public:
    DiskReadWorkerThread(DiskReadThreadPool & manager):
    m_threadID(m_parent.m_nbActiveThread++),m_parent(manager),m_lc(m_parent.m_lc) {
       log::LogContext::ScopedParam param(m_lc, log::Param("threadID", m_threadID));
       m_lc.log(LOG_INFO,"DiskWrite Thread created");
    }
    void startThreads() { start(); }
    void waitThreads() { wait(); }
  private:
    const int m_threadID;
    DiskReadThreadPool & m_parent;
    castor::log::LogContext m_lc;
    
    virtual void run();
  };
  std::vector<DiskReadWorkerThread *> m_threads;
  castor::log::LogContext m_lc;
  MigrationTaskInjector* m_injector;
  const unsigned int m_maxFilesReq;
  const unsigned int m_maxBytesReq;
  
  tape::threading::AtomicCounter<int> m_nbActiveThread;
};

}}}}

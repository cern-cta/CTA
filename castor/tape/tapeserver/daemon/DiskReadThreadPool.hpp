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
  DiskReadThreadPool(int nbThread, int m_maxFilesReq,int m_maxBytesReq, 
          castor::log::LogContext lc);
  ~DiskReadThreadPool();
  void startThreads();
  void waitThreads();
  virtual void push(DiskReadTaskInterface *t);
  void finish();
  void setTaskInjector(MigrationTaskInjector* injector){
      m_injector = injector;
  }
  DiskReadTaskInterface* popAndRequestMore();
private:
  class DiskReadWorkerThread: private castor::tape::threading::Thread {
  public:
    DiskReadWorkerThread(DiskReadThreadPool & manager):
    threadID(m_nbActiveThread++),_this(manager),lc(_this.m_lc) {
       log::LogContext::ScopedParam param(lc, log::Param("threadID", threadID));
       lc.log(LOG_INFO,"DiskWrite Thread created");
    }
    void startThreads() { start(); }
    void waitThreads() { wait(); }
  private:
    static tape::threading::AtomicCounter<int> m_nbActiveThread;
    const int threadID;
    DiskReadThreadPool & _this;
    castor::log::LogContext lc;
    
    virtual void run();
  };
  std::vector<DiskReadWorkerThread *> m_threads;
  castor::log::LogContext m_lc;
  MigrationTaskInjector* m_injector;
  int m_maxFilesReq;
  int m_maxBytesReq;
  tape::threading::Mutex m_loopBackMutex;
};

}}}}

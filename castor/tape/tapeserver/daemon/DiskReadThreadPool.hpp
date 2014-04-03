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
#include <vector>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
class DiskReadThreadPool : public DiskThreadPoolInterface<DiskReadTaskInterface> {
public:
  DiskReadThreadPool(int nbThread){
    for(int i=0; i<nbThread; i++) {
      DiskReadWorkerThread * thr = new DiskReadWorkerThread(*this);
      m_threads.push_back(thr);
    }
  }
  ~DiskReadThreadPool() { 
    while (m_threads.size()) {
      delete m_threads.back();
      m_threads.pop_back();
    }
  }
  void startThreads() {
    for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->startThreads();
    }
  }
  void waitThreads() {
    for (std::vector<DiskReadWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->waitThreads();
    }
  }
  virtual void push(DiskReadTaskInterface *t) { m_tasks.push(t); }
  void finish() {
    /* Insert one endOfSession per thread */
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(NULL);
    }
  }

private:
  class DiskReadWorkerThread: private castor::tape::threading::Thread {
  public:
    DiskReadWorkerThread(DiskReadThreadPool & manager): m_manager(manager) {}
    void startThreads() { start(); }
    void waitThreads() { wait(); }
  private:
    DiskReadThreadPool & m_manager;
    virtual void run() {
      std::auto_ptr<DiskReadTaskInterface> task;
      while(1) {
        task.reset( m_manager.m_tasks.pop());
        if (NULL!=task.get()) 
          task->execute();
        else
          break;
      }
    }
  };
  std::vector<DiskReadWorkerThread *> m_threads;
};

}}}}

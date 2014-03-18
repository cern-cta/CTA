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

#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "DiskThreadPoolInterface.hpp"
#include <vector>

class DiskReadThreadPool : public castor::tape::tapeserver::daemon::DiskThreadPoolInterface<DiskReadTask> {
public:
  DiskReadThreadPool(int nbThread) {
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
  virtual void push(DiskReadTask *t) { m_tasks.push(t); }
  void finish() {
    /* Insert one endOfSession per thread */
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(new endOfSession);
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
      while(1) {
        DiskReadTask * task = m_manager.m_tasks.pop();
        bool end = task->endOfWork();
        if (!end) task->execute();
        delete task;
        if (end) return;
      }
    }
  };
  castor::tape::threading::BlockingQueue<DiskReadTask *> m_tasks;
  std::vector<DiskReadWorkerThread *> m_threads;
};

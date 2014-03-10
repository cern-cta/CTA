/******************************************************************************
 *                      TapeReadSingleThread.hpp
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

#include "castor/tape/tapeserver/daemon/TapeQueue.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/daemon/TapeThreading.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include <iostream>
#include <stdio.h>

class TapeReadSingleThread {
public:
  TapeReadSingleThread(castor::tape::drives::DriveInterface & drive): m_workerThread(*this), m_drive(drive) {}
  void startThreads() { m_workerThread.startThreads(); }
  void waitThreads() { m_workerThread.waitThreads(); } 
  void push(TapeReadTask * t) { m_tasks.push(t); }
  void finish() { m_tasks.push(new endOfSession); }
  
private:
  class endOfSession: public TapeReadTask {
    virtual bool endOfWork() { return true; }
  };
  class TapeReadWorkerThread : private TapeThread {
  public:
    TapeReadWorkerThread(TapeReadSingleThread & manager): m_manager(manager) {}
    void startThreads() { TapeThread::start(); }
    void waitThreads() {
      TapeThread::wait();
    }
  private:
    TapeReadSingleThread & m_manager;
    virtual void run() {
      while(1) {
        TapeReadTask * task = m_manager.m_tasks.pop();
        bool end = task->endOfWork();
        if (!end) task->execute(m_manager.m_drive);
        delete task;
        if (end) {
          printf("End of TapeReadWorkerThread::run()\n");
          return;
        }
      }
    }
  } m_workerThread;
  friend class TapeReadWorkerThread;
  castor::tape::drives::DriveInterface & m_drive;
  BlockingQueue<TapeReadTask *> m_tasks;
};

/******************************************************************************
 *                      DiskWriteThreadPool.hpp
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

#include "castor/tape/tapeserver/daemon/DiskWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/TapeQueue.hpp"
#include "castor/tape/tapeserver/daemon/TapeThreading.hpp"
#include "castor/tape/tapeserver/daemon/JobInjector.hpp"
#include <vector>

class DiskWriteThreadPool {
public:
  DiskWriteThreadPool(int nbThread, int maxFilesReq, int maxBlocksReq):
            m_jobInjector(NULL), m_filesQueued(0), m_blocksQueued(0), 
            m_maxFilesReq(maxFilesReq), m_maxBlocksReq(maxBlocksReq)
   {
    for(int i=0; i<nbThread; i++) {
      DiskWriteWorkerThread * thr = new DiskWriteWorkerThread(*this);
      m_threads.push_back(thr);
    }
  }
  ~DiskWriteThreadPool() { 
    while (m_threads.size()) {
      delete m_threads.back();
      m_threads.pop_back();
    }
  }
  void startThreads() {
    for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->startThreads();
    }
  }
  void waitThreads() {
    for (std::vector<DiskWriteWorkerThread *>::iterator i=m_threads.begin();
            i != m_threads.end(); i++) {
      (*i)->waitThreads();
    }
  }
  void push(DiskWriteTask *t) { 
    {
      TapeMutexLocker ml(&m_counterProtection);
      m_filesQueued += t->files();
      m_blocksQueued += t->blocks();
    }
    m_tasks.push(t);
  }
  void finish() {
    /* Insert one endOfSession per thread */
    for (size_t i=0; i<m_threads.size(); i++) {
      m_tasks.push(new endOfSession);
    }
  }
  void setJobInjector(JobInjector * ji){
    m_jobInjector = ji;
  }

private:
  bool belowMidBlocksAfterPop(int blocksPopped) {
    return m_blocksQueued - blocksPopped < m_maxBlocksReq/2;
  }
  bool belowMidFilesAfterPop(int filesPopped) {
    return m_filesQueued -filesPopped < m_maxFilesReq/2;
  }
   bool crossingDownBlockThreshod(int blockPopped) {
    return (m_blocksQueued >= m_maxBlocksReq/2) && (m_blocksQueued - blockPopped < m_maxBlocksReq/2);
  }
  bool crossingDownFileThreshod(int filesPopped) {
    return (m_filesQueued >= m_maxFilesReq/2) && (m_filesQueued - filesPopped < m_maxFilesReq/2);
  }
  DiskWriteTask * popAndRequestMoreJobs() {
    DiskWriteTask * ret = m_tasks.pop();
    {
      TapeMutexLocker ml(&m_counterProtection);
      /* We are about to go to empty: request a last call job injection */
      if(m_filesQueued == 1 && ret->files()) {
	printf("In DiskWriteTask::popAndRequestMoreJobs(), requesting last call: files=%d, blocks=%d, ret->files=%d, ret->blocks=%d, maxFiles=%d, maxBlocks=%d\n", 
                 m_filesQueued, m_blocksQueued, ret->files(), ret->blocks(), m_maxFilesReq, m_maxBlocksReq);
        m_jobInjector->requestInjection(m_maxFilesReq, m_maxBlocksReq, true);
      } else if (belowMidBlocksAfterPop(ret->blocks()) && belowMidFilesAfterPop(ret->files()) 
	      && (crossingDownBlockThreshod(ret->blocks()) || crossingDownFileThreshod(ret->files()))) {
         printf("In DiskWriteTask::popAndRequestMoreJobs(), requesting: files=%d, blocks=%d, ret->files=%d, ret->blocks=%d, maxFiles=%d, maxBlocks=%d\n", 
                 m_filesQueued, m_blocksQueued, ret->files(), ret->blocks(), m_maxFilesReq, m_maxBlocksReq);
        /* We are crossing the half full queue threshold down: ask for more more */
        m_jobInjector->requestInjection(m_maxFilesReq, m_maxBlocksReq, false);
      }
      m_filesQueued-=ret->files();
      m_blocksQueued-=ret->blocks();
    }
    return ret;
  }
  
  class endOfSession: public DiskWriteTask {
    virtual bool endOfWork() { return true; }
  };
  class DiskWriteWorkerThread: private TapeThread {
  public:
    DiskWriteWorkerThread(DiskWriteThreadPool & manager): m_manager(manager) {}
    void startThreads() { TapeThread::start(); }
    void waitThreads() { TapeThread::wait(); }
  private:
    DiskWriteThreadPool & m_manager;
    virtual void run() {
      while(1) {
        DiskWriteTask * task = m_manager.popAndRequestMoreJobs();
        bool end = task->endOfWork();
        if (!end) task->execute();
        delete task;
        if (end) {
          printf ("Disk write thread finishing\n");
          return;
        }
      }
    }
  };
  BlockingQueue<DiskWriteTask *> m_tasks;
  std::vector<DiskWriteWorkerThread *> m_threads;
  TapeMutex m_counterProtection;
  JobInjector * m_jobInjector;
  int m_filesQueued;
  int m_blocksQueued;
  int m_maxFilesReq;
  int m_maxBlocksReq;

};

/******************************************************************************
 *                      RecallJobInjector.hpp
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

#include <list>
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadFileTask.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteFileTask.hpp"
#include "castor/tape/tapeserver/daemon/RecallJob.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteFileTask.hpp"
#include "castor/tape/tapeserver/daemon/MockTapeGateway.hpp"

class RecallJobInjector: public JobInjector {
public:
  RecallJobInjector(MemoryManager & mm, TapeReadSingleThread & tapeReader,
  DiskWriteThreadPool & diskWriter, MockTapeGateway & tapeGateway):  
	  m_thread(*this), m_mm(mm),
          m_tapeReader(tapeReader), m_diskWriter(diskWriter), 
	  m_tapeGateway(tapeGateway) {}
  
  ~RecallJobInjector() { castor::tape::threading::MutexLocker ml(&m_producerProtection); }
  
  virtual void requestInjection(int maxFiles, int maxBlocks, bool lastCall) {
    printf("RecallJobInjector::requestInjection()\n");
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_queue.push(Request(maxFiles, maxBlocks, lastCall));
  }
  void waitThreads() {
    m_thread.wait();
  }
  void startThreads() {
    m_thread.start();
  }
private:
  void injectBulkRecalls(std::list<RecallJob> jobs) {
    for (std::list<RecallJob>::iterator i=jobs.begin(); i!= jobs.end(); i++) {
      DiskWriteFileTask * dwt = new DiskWriteFileTask(i->fileId, i->blockCount, m_mm);
      TapeReadFileTask * trt = new TapeReadFileTask(*dwt, i->fSeq, i->blockCount);
      m_diskWriter.push(dwt);
      m_tapeReader.push(trt);
    }
  }
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(RecallJobInjector & rji): m_parent(rji) {}
    void run() {
      while (1) {
        Request req = m_parent.m_queue.pop();
	printf("RecallJobInjector:run: about to call gateway\n");
        std::list<RecallJob> jobs = m_parent.m_tapeGateway.getMoreWork(req.maxFiles, req.maxBlocks);
        if (!jobs.size()) {
          if (req.lastCall) {
	    printf("In RecallJobInjector::WorkerThread::run(): last call turned up empty: triggering the end of session.\n");
            m_parent.m_tapeReader.finish();
            m_parent.m_diskWriter.finish();
            break;
          } else {
	    printf("In RecallJobInjector::WorkerThread::run(): got empty list, but not last call. NoOp.\n");
	  }
        } else {
	  printf("In RecallJobInjector::WorkerThread::run(): got %zd files.\n", jobs.size());
          m_parent.injectBulkRecalls(jobs);
        }
      }
      try {
        while(1) {
          Request req = m_parent.m_queue.tryPop();
          printf("In RecallJobInjector::WorkerThread::run(): popping extra request (lastCall=%d)\n", req.lastCall);
        }
      } catch (castor::tape::threading::noMore) {
        printf("In RecallJobInjector::WorkerThread::run(): Drained the request queue. We're now empty. Finishing.\n");
      }
    }
  private:
    RecallJobInjector & m_parent;
      
  } m_thread;
  friend class WorkerThread;
  class Request {
  public:
    Request(int mf, int mb, bool lc): maxFiles(mf), maxBlocks(mb), lastCall(lc) {}
    int maxFiles;
    int maxBlocks;
    bool lastCall;
  };
  MemoryManager & m_mm;
  TapeReadSingleThread & m_tapeReader;
  DiskWriteThreadPool & m_diskWriter;
  MockTapeGateway & m_tapeGateway;
  castor::tape::threading::Mutex m_producerProtection;
  castor::tape::threading::BlockingQueue<Request> m_queue;
};

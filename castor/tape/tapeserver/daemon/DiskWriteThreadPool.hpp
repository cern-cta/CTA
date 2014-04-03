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

#include "castor/tape/tapeserver/daemon/DiskWriteTaskInterface.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
#include "castor/tape/tapeserver/daemon/TaskInjector.hpp"
#include "DiskThreadPoolInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include <vector>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

class DiskWriteThreadPool : public DiskThreadPoolInterface<DiskWriteTaskInterface> {
public:
  DiskWriteThreadPool(int nbThread, int maxFilesReq, int maxBlocksReq,castor::log::LogContext lc);
  ~DiskWriteThreadPool();
  
  void startThreads();
  void waitThreads();
  virtual void push(DiskWriteTaskInterface *t);
  void finish();
  void setJobInjector(TaskInjector * ji);

private:
  bool belowMidBlocksAfterPop(int blocksPopped) ;
  bool belowMidFilesAfterPop(int filesPopped) ;
  bool crossingDownBlockThreshod(int blockPopped);
  bool crossingDownFileThreshod(int filesPopped) ;
  DiskWriteTaskInterface * popAndRequestMoreJobs() ;
  
  class DiskWriteWorkerThread: private castor::tape::threading::Thread {
  public:
    DiskWriteWorkerThread(DiskWriteThreadPool & manager): _this(manager) {
      ++m_nbActiveThread;
    }
    void startThreads() { castor::tape::threading::Thread::start(); }
    void waitThreads() { castor::tape::threading::Thread::wait(); }
  private:
    static tape::threading::AtomicCounter<int> m_nbActiveThread;
    DiskWriteThreadPool & _this;
    virtual void run();
  };
  
  std::vector<DiskWriteWorkerThread *> m_threads;
  castor::tape::threading::Mutex m_counterProtection;
  TaskInjector * m_jobInjector;
  //
  uint32_t m_filesQueued;
  
  // 
  uint32_t m_blocksQueued;
  
  uint32_t m_maxFilesReq;
  uint64_t m_maxBytesReq;
  castor::log::LogContext m_lc;
};

}}}}

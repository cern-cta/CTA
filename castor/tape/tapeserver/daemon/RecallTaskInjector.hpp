/******************************************************************************
 *                      RecallTaskInjector.hpp
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
#include <stdint.h>

#include "MemManager.hpp"
#include "TapeReadSingleThread.hpp"
#include "TapeReadFileTask.hpp"
#include "DiskWriteThreadPool.hpp"
#include "DiskWriteFileTask.hpp"
#include "RecallJob.hpp"
#include "TapeWriteFileTask.hpp"
#include "ClientInterface.hpp"
#include "castor/log/LogContext.hpp"


using castor::log::LogContext;

namespace castor{
namespace tape{
namespace tapeserver{
namespace daemon {

/**
 * This classis reponsible for creating the tasks in case of a recall job
  */
class RecallTaskInjector: public JobInjector {
public:

  RecallTaskInjector(MemoryManager & mm, 
        TapeReadSingleThread & tapeReader,
        DiskWriteThreadPool & diskWriter,ClientInterface& client,
        castor::log::LogContext& lc);


  virtual void requestInjection(int maxFiles, int maxBlocks, bool lastCall);

  bool synchronousInjection(uint64_t maxFiles, uint64_t maxBlocks);

  void waitThreads();
  void startThreads();
private:
  void injectBulkRecalls(const std::list<RecallJob>& jobs);

  /**
   * A request of files to recall
   */
  class Request {
  public:
    Request(int mf, int mb, bool lc): maxFiles(mf), maxBlocks(mb), lastCall(lc) {}
    const int maxFiles;
    const int maxBlocks;
    const bool lastCall;
  };
  
  class WorkerThread: public castor::tape::threading::Thread {
  public:
    WorkerThread(RecallTaskInjector & rji): _this(rji) {}
    void run();
  private:
    RecallTaskInjector & _this;

  } m_thread;

  MemoryManager & m_memManager;
  TapeReadSingleThread & m_tapeReader;
  DiskWriteThreadPool & m_diskWriter;

  ClientInterface& m_client;
  castor::log::LogContext& m_lc;
  castor::tape::threading::Mutex m_producerProtection;
  castor::tape::threading::BlockingQueue<Request> m_queue;
};

} //end namespace daemon
} //end namespace tapeserver
} //end namespace tape
} //end namespace castor

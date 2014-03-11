/******************************************************************************
 *                      TapeWriteSingleThread.hpp
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

#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/MigrationJob.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include <iostream>
#include <stdio.h>

class TapeWriteSingleThread {
public:
  TapeWriteSingleThread(castor::tape::drives::DriveInterface & drive, MigrationReportPacker & repPacker,
	  int filesBeforeFlush, int blockBeforeFlush): 
  m_workerThread(*this), m_drive(drive), m_filesBeforeFlush(filesBeforeFlush),
  m_blocksBeforeFlush(blockBeforeFlush), m_reportPacker(repPacker) {}
  void startThreads() { m_workerThread.startThreads(); }
  void waitThreads() { m_workerThread.waitThreads(); } 
  void push(TapeWriteTask * t) { m_tasks.push(t); }
  void finish() { m_tasks.push(new endOfSession); }
  
private:
  class endOfSession: public TapeWriteTask {
    virtual bool endOfWork() { return true; }
  };
  class TapeWriteWorkerThread : private castor::tape::threading::Thread {
  public:
    TapeWriteWorkerThread(TapeWriteSingleThread & manager): m_manager(manager) {}
    void startThreads() { start(); }
    void waitThreads() {
      wait();
    }
  private:
    TapeWriteSingleThread & m_manager;
    virtual void run() {
      int blocks=0;
      int files=0;
      while(1) {
        TapeWriteTask * task = m_manager.m_tasks.pop();
        bool end = task->endOfWork();
        if (!end) task->execute(m_manager.m_drive);
	m_manager.m_reportPacker.reportCompletedJob(MigrationJob(-1, task->fSeq(), -1));
	files+=task->files();
	blocks+=task->blocks();
	if (files >= m_manager.m_filesBeforeFlush ||
		blocks >= m_manager.m_blocksBeforeFlush) {
	  printf("Flushing after %d files and %d blocks\n", files, blocks);
	  m_manager.m_reportPacker.reportFlush();
	  files=0;
	  blocks=0;
	  m_manager.m_drive.flush();
	}
        delete task;
        if (end) {
          printf("End of TapeWriteWorkerThread::run() (flushing)\n");
	  m_manager.m_drive.flush();
	  m_manager.m_reportPacker.reportEndOfSession();
          return;
        }
      }
    }
  } m_workerThread;
  friend class TapeWriteWorkerThread;
  castor::tape::drives::DriveInterface & m_drive;
  castor::tape::threading::BlockingQueue<TapeWriteTask *> m_tasks;
  int m_filesBeforeFlush;
  int m_blocksBeforeFlush;
  MigrationReportPacker & m_reportPacker;
};

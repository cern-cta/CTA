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
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include <iostream>
#include <stdio.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

class TapeWriteSingleThread :  public TapeSingleThreadInterface<TapeWriteTask> {
public:
  TapeWriteSingleThread(castor::tape::drives::DriveInterface & drive, 
          const std::string & vid,
          castor::log::LogContext & lc,
          MigrationReportPacker & repPacker,
	  int filesBeforeFlush, int blockBeforeFlush): 
  TapeSingleThreadInterface<TapeWriteTask>(drive, vid, lc),
   m_filesBeforeFlush(filesBeforeFlush),
   m_blocksBeforeFlush(blockBeforeFlush),
   m_reportPacker(repPacker) {}

private:
  virtual void run() {
      int blocks=0;
      int files=0;
      while(1) {
        TapeWriteTask * task = m_tasks.pop();
        bool end = task->endOfWork();
        if (!end) task->execute(m_drive);
	//m_reportPacker.reportCompletedJob(MigrationJob(-1, task->fSeq(), -1));
	files+=task->files();
	blocks+=task->blocks();
        m_filesProcessed += task->files();
	if (files >= m_filesBeforeFlush ||
		blocks >= m_blocksBeforeFlush) {
	  printf("Flushing after %d files and %d blocks\n", files, blocks);
	  m_reportPacker.reportFlush();
	  files=0;
	  blocks=0;
	  m_drive.flush();
	}
        delete task;
        if (end) {
          printf("End of TapeWriteWorkerThread::run() (flushing)\n");
	  m_drive.flush();
	  m_reportPacker.reportEndOfSession();
          return;
        }
      }
    }
      
  int m_filesBeforeFlush;
  int m_blocksBeforeFlush;
  
  MigrationReportPacker & m_reportPacker;
};
}}}}

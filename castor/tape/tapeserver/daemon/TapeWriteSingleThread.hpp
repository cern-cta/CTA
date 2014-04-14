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

class TapeWriteSingleThread :  public TapeSingleThreadInterface<TapeWriteTaskInterface> {
public:
  TapeWriteSingleThread(castor::tape::drives::DriveInterface & drive, MigrationReportPacker & repPacker,
	  int filesBeforeFlush, int blockBeforeFlush,castor::log::LogContext lc): 
  TapeSingleThreadInterface<TapeWriteTaskInterface>(drive),
   m_filesBeforeFlush(filesBeforeFlush),m_blocksBeforeFlush(blockBeforeFlush), 
      m_drive(drive),m_reportPacker(repPacker),m_lc(lc)
  {}

private:
  virtual void run() {
    // First we have to initialise the tape read session
    std::auto_ptr<castor::tape::tapeFile::ReadSession> rs;
    try {
      rs.reset(new castor::tape::tapeFile::ReadSession(m_drive, m_vid));
      m_lc.log(LOG_INFO, "Tape Write session session successfully started");
    }
    catch (castor::exception::Exception & ex) {
      m_lc.log(LOG_ERR, "Failed to start tape read session");
      // TODO: log and unroll the session
      // TODO: add an unroll mode to the tape read task. (Similar to exec, but pushing blocks marked in error)
    }

      int blocks=0;
      int files=0;
      std::auto_ptr<TapeWriteTaskInterface> task ;      
      while(1) {
        task.reset(m_tasks.pop());
        
        if(NULL!=task.get()) {
          task->execute(*rs);
          files++;
          blocks+=task->blocks();
        
          if (files >= m_filesBeforeFlush || blocks >= m_blocksBeforeFlush) {
            m_drive.flush();
            log::LogContext::ScopedParam sp0(m_lc, log::Param("files", files));
            log::LogContext::ScopedParam sp1(m_lc, log::Param("blocks", blocks));
            m_lc.log(LOG_INFO,"Flushing after files and blocks");
            m_reportPacker.reportFlush();
            files=0;
            blocks=0;
          }
        }
        else{
          m_lc.log(LOG_INFO,"End of TapeWriteWorkerThread::run() (flushing");
	  m_drive.flush();
	  m_reportPacker.reportEndOfSession();
          return;
        }
      }
  }
      
  int m_filesBeforeFlush;
  int m_blocksBeforeFlush;
  
  castor::tape::drives::DriveInterface& m_drive;
  MigrationReportPacker & m_reportPacker;
  castor::log::LogContext m_lc;
};
}}}}

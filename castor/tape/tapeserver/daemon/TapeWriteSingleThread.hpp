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
          int filesBeforeFlush, int blockBeforeFlush,
  const std::string vid, uint32_t lastFseq,bool compress, castor::log::LogContext lc): 
  TapeSingleThreadInterface<TapeWriteTaskInterface>(drive),
   m_filesBeforeFlush(filesBeforeFlush),m_blocksBeforeFlush(blockBeforeFlush), 
      m_drive(drive),m_reportPacker(repPacker),m_vid(vid),m_lastFseq(lastFseq),m_compress(compress),m_lc(lc)
  {}

private:
  /**
   * Function to open the WriteSession 
   * If successful, returns a std::auto_ptr on it. A copy of that std::auto_ptr
   * will give the caller the ownership of the opened session (see auto_ptr 
   * copy constructor, which has a move semantic)
   * @return 
   */
  std::auto_ptr<castor::tape::tapeFile::WriteSession> openWriteSession() {
    using castor::log::LogContext;
    using castor::log::Param;
    typedef LogContext::ScopedParam ScopedParam;
    
    std::auto_ptr<castor::tape::tapeFile::WriteSession> rs;
  
    ScopedParam sp[]={
        ScopedParam(m_lc, Param("vid",m_vid)),
        ScopedParam(m_lc, Param("lastFseq", m_lastFseq)),
        ScopedParam(m_lc, Param("compression", m_compress)) 
      };
    tape::utils::suppresUnusedVariable(sp);
      try {
       rs.reset(new castor::tape::tapeFile::WriteSession(m_drive,m_vid,m_lastFseq,m_compress));
        m_lc.log(LOG_INFO, "Tape Write session session successfully started");
      }
      catch (castor::exception::Exception & ex) {
        m_lc.log(LOG_ERR, "Failed to start tape read session");
        // TODO: log and unroll the session
        // TODO: add an unroll mode to the tape read task. (Similar to exec, but pushing blocks marked in error)
        throw;
      }
    return rs;
  }
  
  void flush(const std::string& message,int blocks,int files){
    m_drive.flush();
    log::LogContext::ScopedParam sp0(m_lc, log::Param("files", files));
    log::LogContext::ScopedParam sp1(m_lc, log::Param("blocks", blocks));
    m_lc.log(LOG_INFO,message);
    m_reportPacker.reportFlush();
  }
  
  virtual void run() {
    try
    {
      // First we have to initialise the tape read session
      std::auto_ptr<castor::tape::tapeFile::WriteSession> rs(openWriteSession());
    
      int blocks=0;
      int files=0;
      std::auto_ptr<TapeWriteTaskInterface> task ;      
      while(1) {
        task.reset(m_tasks.pop());
        
        if(NULL!=task.get()) {
          task->execute(*rs,m_lc);
          files++;
          blocks+=task->blocks();
          
          if (files >= m_filesBeforeFlush || blocks >= m_blocksBeforeFlush) {
            flush("Normal flush because thresholds was reached",blocks,files);
            files=0;
            blocks=0;
          }
        }
        else{
          flush("End of TapeWriteWorkerThread::run() (flushing",blocks,files);
          m_reportPacker.reportEndOfSession();
          return;
        }
      } 
    }
    catch(const castor::exception::Exception& e){
      log::LogContext::ScopedParam sp1(m_lc, log::Param("what", e.what()));
      log::LogContext::ScopedParam sp2(m_lc, log::Param("error_MessageValue", e.getMessageValue()));
      m_lc.log(LOG_INFO,"An error occurred during TapwWriteSingleThread::execute");
      m_reportPacker.reportEndOfSessionWithErrors(e.what(),e.code());
    }
  }
  
  int m_filesBeforeFlush;
  int m_blocksBeforeFlush;
  
  castor::tape::drives::DriveInterface& m_drive;
  MigrationReportPacker & m_reportPacker;
  const std::string m_vid;
  const uint32_t m_lastFseq;
  const bool m_compress;
  
  castor::log::LogContext m_lc;
};
}}}}

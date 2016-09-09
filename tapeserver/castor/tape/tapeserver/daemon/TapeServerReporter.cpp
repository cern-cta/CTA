/****************************************************************************** 
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

#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/TpconfigLine.hpp"

#include <sys/types.h>
#include <unistd.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//-----------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------  
TapeServerReporter::TapeServerReporter(
  cta::tape::daemon::TapedProxy& tapeserverProxy,
  const DriveConfig& driveConfig,
  const std::string &hostname,
  const castor::tape::tapeserver::daemon::VolumeInfo &volume,
  cta::log::LogContext lc):
  m_threadRunnig(false),
  m_tapeserverProxy(tapeserverProxy),
  m_lc(lc),
  m_server(hostname),
  m_unitName(driveConfig.getUnitName()),
  m_logicalLibrary(driveConfig.getLogicalLibrary()),
  m_volume(volume),
  m_sessionPid(getpid()){
  //change the thread's name in the log
  m_lc.pushOrReplace(cta::log::Param("thread","TapeServerReporter"));
}
  
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------
void TapeServerReporter::finish(){
  m_fifo.push(NULL);
}

//------------------------------------------------------------------------------
//startThreads
//------------------------------------------------------------------------------   
void TapeServerReporter::startThreads(){
  start();
  m_threadRunnig=true;
}

//------------------------------------------------------------------------------
//waitThreads
//------------------------------------------------------------------------------     
void TapeServerReporter::waitThreads(){
  try{
    wait();
    m_threadRunnig=false;
  }catch(const std::exception& e){
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("what",e.what());
      m_lc.log(cta::log::ERR,"error caught while waiting");
    }catch(...){
      m_lc.log(cta::log::ERR,"unknown error while waiting");
    }
}

//------------------------------------------------------------------------------
//reportState
//------------------------------------------------------------------------------  
void TapeServerReporter::reportState(cta::tape::session::SessionState state,
  cta::tape::session::SessionType type) {
  m_fifo.push(new ReportStateChange(state, type));
}

//------------------------------------------------------------------------------
//reportTapeUnmountedForRetrieve
//------------------------------------------------------------------------------ 
void TapeServerReporter::reportTapeUnmountedForRetrieve() {
  m_fifo.push(new ReportTapeUnmountedForRetrieve());
}

//------------------------------------------------------------------------------
//reportDiskCompleteForRetrieve
//------------------------------------------------------------------------------ 
void TapeServerReporter::reportDiskCompleteForRetrieve() {
  m_fifo.push(new ReportDiskCompleteForRetrieve());
}

//------------------------------------------------------------------------------
//run
//------------------------------------------------------------------------------  
void TapeServerReporter::run(){
  while(1){
    std::unique_ptr<Report> currentReport(m_fifo.pop());
    if(NULL==currentReport.get()) {
      break;
    }
    try{
      currentReport->execute(*this); 
    }catch(const std::exception& e){
      cta::log::ScopedParamContainer sp(m_lc);
      sp.add("what",e.what());
      m_lc.log(cta::log::ERR,"TapeServerReporter error caught");
    }
  }
}

//------------------------------------------------------------------------------
// ReportStateChange::ReportStateChange())
//------------------------------------------------------------------------------   
TapeServerReporter::ReportStateChange::ReportStateChange(cta::tape::session::SessionState state,
  cta::tape::session::SessionType type): m_state(state), m_type(type) { }

//------------------------------------------------------------------------------
// ReportStateChange::execute())
//------------------------------------------------------------------------------  
void TapeServerReporter::ReportStateChange::execute(TapeServerReporter& parent) {
  parent.m_tapeserverProxy.reportState(m_state, m_type, parent.m_volume.vid);
}

//------------------------------------------------------------------------------
// ReportTapeUnmountedForRetrieve::execute())
//------------------------------------------------------------------------------  
void TapeServerReporter::ReportTapeUnmountedForRetrieve::execute(TapeServerReporter& parent) {
  parent.m_tapeUnmountedForRecall=true;
  if (parent.m_diskCompleteForRecall) {
    parent.m_tapeserverProxy.reportState(cta::tape::session::SessionState::Shutdown, 
      cta::tape::session::SessionType::Retrieve, parent.m_volume.vid);
  } else {
    parent.m_tapeserverProxy.reportState(cta::tape::session::SessionState::DrainingToDisk, 
      cta::tape::session::SessionType::Retrieve, parent.m_volume.vid);
  }
}

//------------------------------------------------------------------------------
// ReportDiskCompleteForRetrieve::execute())
//------------------------------------------------------------------------------ 
void TapeServerReporter::ReportDiskCompleteForRetrieve::execute(TapeServerReporter& parent) {
  parent.m_diskCompleteForRecall=true;
  if (parent.m_tapeUnmountedForRecall) {
    parent.m_tapeserverProxy.reportState(cta::tape::session::SessionState::Shutdown, 
      cta::tape::session::SessionType::Retrieve, parent.m_volume.vid);
  }
}

}}}} // namespace castor::tape::tapeserver::daemon


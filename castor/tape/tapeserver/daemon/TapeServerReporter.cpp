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

#include "castor/log/LogContext.hpp"
#include "castor/log/Logger.hpp"
#include "castor/messages/TapeserverProxy.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/TpconfigLine.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//-----------------------------------------------------------------------------
//constructor
//------------------------------------------------------------------------------  
TapeServerReporter::TapeServerReporter(
  messages::TapeserverProxy& tapeserverProxy,
  const DriveConfig& driveConfig,
  const std::string &hostname,
  const castor::tape::tapeserver::client::ClientInterface::VolumeInfo &volume,
  log::LogContext lc):
  m_threadRunnig(false),
  m_tapeserverProxy(tapeserverProxy),
  m_lc(lc),
  m_server(hostname),
  m_unitName(driveConfig.unitName),
  m_dgn(driveConfig.dgn),
  m_volume(volume),
  m_sessionPid(getpid()){
  //change the thread's name in the log
  m_lc.pushOrReplace(log::Param("thread","TapeServerReporter"));
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
        log::ScopedParamContainer sp(m_lc);
        sp.add("what",e.what());
        m_lc.log(LOG_ERR,"error caught while waiting");
      }catch(...){
        m_lc.log(LOG_ERR,"unknown error while waiting");
      }
  }
//------------------------------------------------------------------------------
//tapeMountedForWrite
//------------------------------------------------------------------------------    
  void TapeServerReporter::tapeMountedForWrite(){
    m_fifo.push(
    new ReportTapeMounterForWrite()
    );
  }
//------------------------------------------------------------------------------
//gotWriteMountDetailsFromClient
//------------------------------------------------------------------------------    
  uint32_t TapeServerReporter::gotWriteMountDetailsFromClient(){
    if(m_threadRunnig){
      m_lc.log(LOG_ERR,"TapeServerReporter is running but calling a synchronous operation on it"
      "Could cause a race with the underlying  zmq sockets in the proxy");
    }
    switch(m_volume.clientType) {
    case tapegateway::TAPE_GATEWAY:
      return m_tapeserverProxy.gotMigrationJobFromTapeGateway(m_volume.vid,
        m_unitName);
    case tapegateway::WRITE_TP:
      return m_tapeserverProxy.gotMigrationJobFromWriteTp(m_volume.vid,
        m_unitName);
    default:
      {
        // Should never get here
        castor::exception::Exception ex;
        ex.getMessage() << "Failed to notify tapeserverd of write mount details"
          " received from client: Unexpected client type: clientType=" <<
          m_volume.clientType;
        throw ex;
      }
    } // switch(m_volume.clientType)
  }
//------------------------------------------------------------------------------
//gotReadMountDetailsFromClient
//------------------------------------------------------------------------------  
   void TapeServerReporter::gotReadMountDetailsFromClient(){
     m_fifo.push(
     new ReportGotReadDetailsFromClient()
     );
   }
//------------------------------------------------------------------------------
//tapeMountedForRead
//------------------------------------------------------------------------------  
   void TapeServerReporter::tapeMountedForRead(){
     m_fifo.push(new ReportTapeMountedForRead());
   }
//------------------------------------------------------------------------------
//tapeUnmounted
//------------------------------------------------------------------------------     
   void TapeServerReporter::tapeUnmounted(){
     m_fifo.push(new ReportTapeUnmounted());
   }
//------------------------------------------------------------------------------
//run
//------------------------------------------------------------------------------  
  void TapeServerReporter::run(){
    while(1){
      std::auto_ptr<Report> currentReport(m_fifo.pop());
      if(NULL==currentReport.get()) {
        break;
      }
      try{
        currentReport->execute(*this); 
      }catch(const std::exception& e){
        log::ScopedParamContainer sp(m_lc);
        sp.add("what",e.what());
        m_lc.log(LOG_ERR,"TapeServerReporter error caught");
      }
    }
  }
//------------------------------------------------------------------------------
// ReportGotReadDetailsFromClient::execute
//------------------------------------------------------------------------------
    void TapeServerReporter::ReportGotReadDetailsFromClient::execute(
    TapeServerReporter& parent){
      log::ScopedParamContainer sp(parent.m_lc);
      sp.add("unitName", parent.m_unitName)
        .add("TPVID", parent.m_volume.vid);

      switch(parent.m_volume.clientType) {
      case tapegateway::TAPE_GATEWAY:
        return parent.m_tapeserverProxy.gotRecallJobFromTapeGateway(
          parent.m_volume.vid, parent.m_unitName);
      case tapegateway::READ_TP:
        return parent.m_tapeserverProxy.gotRecallJobFromReadTp(
          parent.m_volume.vid, parent.m_unitName);
      default:
        {
          // Should never get here
          castor::exception::Exception ex;
          ex.getMessage() <<
            "Failed to notify tapeserverd of read mount details received from"
            " client: Unexpected client type: clientType=" <<
            parent.m_volume.clientType;
          throw ex;
        }
      } // switch(parent.m_volume.clientType)
    }
//------------------------------------------------------------------------------
// ReportTapeMountedForRead::execute
//------------------------------------------------------------------------------        
    void TapeServerReporter::ReportTapeMountedForRead::
    execute(TapeServerReporter& parent){
      parent.m_tapeserverProxy.tapeMountedForRecall(parent.m_volume.vid,
        parent.m_unitName);
    }
//------------------------------------------------------------------------------
// ReportTapeUnmounted::execute
//------------------------------------------------------------------------------        
    void TapeServerReporter::ReportTapeUnmounted::
    execute(TapeServerReporter& parent){
      parent.m_tapeserverProxy.tapeUnmounted(parent.m_volume.vid,
        parent.m_unitName);
    }
//------------------------------------------------------------------------------
// ReportTapeMounterForWrite::execute
//------------------------------------------------------------------------------         
    void TapeServerReporter::ReportTapeMounterForWrite::
    execute(TapeServerReporter& parent){
      parent.m_tapeserverProxy.tapeMountedForMigration(parent.m_volume.vid,
        parent.m_unitName);
    }
}}}}



#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/tape/tapeserver/daemon/GlobalStatusReporter.hpp"
#include "castor/tape/utils/TpconfigLine.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/Logger.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//-----------------------------------------------------------------------------
//GlobalStatusReporter::GlobalStatusReporter
//------------------------------------------------------------------------------  
GlobalStatusReporter::GlobalStatusReporter(
  legacymsg::TapeserverProxy& tapeserverProxy,
  legacymsg::VdqmProxy& vdqmProxy,
  const tape::utils::TpconfigLine& configLine,
  const std::string &hostname,
  const castor::tape::tapeserver::client::ClientInterface::VolumeInfo &volume,
  log::LogContext lc):
  m_tapeserverProxy(tapeserverProxy),m_vdqmProxy(vdqmProxy),
  m_lc(lc),
  m_server(hostname),
  m_unitName(configLine.unitName),
  m_dgn(configLine.dgn),
  m_volume(volume),
  m_sessionPid(getpid()){
  //change the thread's name in the log
  m_lc.pushOrReplace(log::Param("thread","GlobalStatusReporter"));
}
  
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------
  void GlobalStatusReporter::finish(){
    m_fifo.push(NULL);
  }
//------------------------------------------------------------------------------
//startThreads
//------------------------------------------------------------------------------   
  void GlobalStatusReporter::startThreads(){
    start();
  }
//------------------------------------------------------------------------------
//waitThreads
//------------------------------------------------------------------------------     
  void GlobalStatusReporter::waitThreads(){
    wait();
  }
  
//------------------------------------------------------------------------------
//gotReadMountDetailsFromClient
//------------------------------------------------------------------------------  
   void GlobalStatusReporter::gotReadMountDetailsFromClient(){
     m_fifo.push(
     new ReportGotDetailsFromClient()
     );
   }
//------------------------------------------------------------------------------
//tapeMountedForRead
//------------------------------------------------------------------------------  
   void GlobalStatusReporter::tapeMountedForRead(){
     m_fifo.push(new ReportTapeMountedForRead());
   }
//------------------------------------------------------------------------------
//tapeUnmounted
//------------------------------------------------------------------------------     
   void GlobalStatusReporter::tapeUnmounted(){
     m_fifo.push(new ReportTapeUnmounted());
   }
//------------------------------------------------------------------------------
//run
//------------------------------------------------------------------------------  
  void GlobalStatusReporter::run(){
    while(1){
      std::auto_ptr<Report> currentReport(m_fifo.pop());
      if(NULL==currentReport.get()) {
        break;
      }
      currentReport->execute(*this); 
    }
  }
//------------------------------------------------------------------------------
// ReportGotDetailsFromClient::execute
//------------------------------------------------------------------------------
    void GlobalStatusReporter::ReportGotDetailsFromClient::execute(
    GlobalStatusReporter& parent){
      log::ScopedParamContainer sp(parent.m_lc);
      sp.add(parent.m_unitName, "unitName").add(parent.m_volume.vid, "vid");
      parent.m_tapeserverProxy.gotWriteMountDetailsFromClient(parent.m_volume, parent.m_unitName);
      parent.m_lc.log(LOG_INFO,"From GlobalStatusReporter, Reported gotWriteMountDetailsFromClient");
    }
//------------------------------------------------------------------------------
// ReportTapeMountedForRead::execute
//------------------------------------------------------------------------------        
    void GlobalStatusReporter::ReportTapeMountedForRead::
    execute(GlobalStatusReporter& parent){
      parent.m_tapeserverProxy.tapeMountedForRead(parent.m_volume, parent.m_unitName);
      parent.m_vdqmProxy.tapeMounted(parent.m_server, parent.m_unitName, parent.m_dgn, parent.m_volume.vid, parent.m_sessionPid);
    }
//------------------------------------------------------------------------------
// ReportTapeUnmounted::execute
//------------------------------------------------------------------------------        
    void GlobalStatusReporter::ReportTapeUnmounted::
    execute(GlobalStatusReporter& parent){
      parent.m_tapeserverProxy.tapeUnmounted(parent.m_volume, parent.m_unitName);
    }
}}}}



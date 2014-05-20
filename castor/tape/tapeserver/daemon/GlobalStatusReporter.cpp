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
  legacymsg::VdqmProxy& vdqmProxy,legacymsg::VmgrProxy& vmgrProxy,
        const tape::utils::TpconfigLine& configLine,const std::string &hostname,
          const std::string &_vid,log::LogContext lc):
          m_tapeserverProxy(tapeserverProxy),m_vdqmProxy(vdqmProxy),
          m_vmgrProxy(vmgrProxy),m_lc(lc), server(hostname),unitName(configLine.unitName),
  dgn(configLine.dgn),vid(_vid),
  sessionPid(getpid()){
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
//finish
//------------------------------------------------------------------------------   
  void GlobalStatusReporter::startThreads(){
    start();
  }
//------------------------------------------------------------------------------
//finish
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
      sp.add(parent.unitName,"unitName").add(parent.vid,"vid");
      parent.m_tapeserverProxy.gotWriteMountDetailsFromClient(parent.unitName,parent.vid);
      parent.m_lc.log(LOG_INFO,"From GlobalStatusReporter, Reported gotWriteMountDetailsFromClient");
    }
//------------------------------------------------------------------------------
// ReportTapeMountedForRead::execute
//------------------------------------------------------------------------------        
    void GlobalStatusReporter::ReportTapeMountedForRead::
    execute(GlobalStatusReporter& parent){
      parent.m_tapeserverProxy.tapeMountedForRead(parent.unitName,parent.vid);
      parent.m_vdqmProxy.tapeMounted(parent.server, parent.unitName, parent.dgn,parent.vid,parent.sessionPid);
    }
//------------------------------------------------------------------------------
// ReportTapeUnmounted::execute
//------------------------------------------------------------------------------        
    void GlobalStatusReporter::ReportTapeUnmounted::
    execute(GlobalStatusReporter& parent){
      parent.m_tapeserverProxy.tapeUnmounted(parent.unitName,parent.vid);
    }
}}}}



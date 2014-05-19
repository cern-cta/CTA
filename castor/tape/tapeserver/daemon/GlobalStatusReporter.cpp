#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/tape/tapeserver/daemon/GlobalStatusReporter.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//------------------------------------------------------------------------------
//GlobalStatusReporter::Report:: Constructors
//------------------------------------------------------------------------------  
GlobalStatusReporter::Report::Report(const std::string &_server,const std::string &_unitName, 
        const std::string &_dgn, const uint32_t _mountTransactionId,
        const pid_t _sessionPid):
        server(_server),unitName(_unitName),dgn(_dgn),
        mountTransactionId(_mountTransactionId),sessionPid(_sessionPid)
  {}
  
 GlobalStatusReporter::Report::Report(const std::string &_server,const std::string &_unitName, 
            const std::string &_dgn,const pid_t _sessionPid):
            server(_server),unitName(_unitName),dgn(_dgn),
         mountTransactionId(0),sessionPid(_sessionPid)
  {}
 
 GlobalStatusReporter::Report::Report(const std::string &_server,
      const std::string &_unitName, const std::string &_dgn):
      server(_server),unitName(_unitName),dgn(_dgn),mountTransactionId(0),sessionPid(0)
  {}
 
  GlobalStatusReporter::Report::Report(const std::string &_unitName):
  server(""),unitName(_unitName),
          dgn(""),mountTransactionId(0),sessionPid(0)
  {}
//------------------------------------------------------------------------------
//GlobalStatusReporter::GlobalStatusReporter
//------------------------------------------------------------------------------  
  GlobalStatusReporter::GlobalStatusReporter(
  legacymsg::TapeserverProxy& tapeserverProxy,
  legacymsg::VdqmProxy& vdqmProxy,legacymsg::VmgrProxy& vmgrProxy,
          log::LogContext lc):
          m_tapeserverProxy(tapeserverProxy),m_vdqmProxy(vdqmProxy),
          m_vmgrProxy(vmgrProxy),m_lc(lc){
  }
  
//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------
   void GlobalStatusReporter::finish(){
        m_fifo.push(NULL);
  }
//------------------------------------------------------------------------------
//reportNowOccupiedDrive
//------------------------------------------------------------------------------   
   void GlobalStatusReporter::reportNowOccupiedDrive(const std::string &server,
           const std::string &unitName,const std::string &dgn, 
           const uint32_t mountTransactionId,const pid_t sessionPid){
     m_fifo.push(
     new ReportAssignDrive(server,unitName,dgn,mountTransactionId,sessionPid)
      );
   }
//------------------------------------------------------------------------------
//gotReadMountDetailsFromClient
//------------------------------------------------------------------------------  
   void GlobalStatusReporter::gotReadMountDetailsFromClient(
   const std::string &unitName,const std::string &vid){
     m_fifo.push(
     new ReportGotDetailsFromClient(unitName,vid)
     );
   }
//------------------------------------------------------------------------------
//tapeMountedForRead
//------------------------------------------------------------------------------  
   void GlobalStatusReporter::tapeMountedForRead(const std::string &server, 
           const std::string &unitName,const std::string &dgn, 
           const std::string &vid, const pid_t sessionPid){
     m_fifo.push(new ReportTapeMountedForRead(server,unitName,dgn,vid,sessionPid));
   }
//------------------------------------------------------------------------------
//tapeUnmounted
//------------------------------------------------------------------------------     
   void GlobalStatusReporter::tapeUnmounted(const std::string &server, const std::string &unitName, 
  const std::string &dgn, const std::string &vid){
     m_fifo.push(new ReportTapeUnmounted(server,unitName,dgn,vid));
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
//ReportAssignDrive::ReportAssignDrive
//------------------------------------------------------------------------------  
  GlobalStatusReporter::ReportAssignDrive::ReportAssignDrive(
      const std::string &_server,const std::string &_unitName, 
            const std::string &_dgn, const uint32_t _mountTransactionId,
            const pid_t _sessionPid):Report(_server,_unitName, _dgn, _mountTransactionId,_sessionPid)
  {}

//------------------------------------------------------------------------------
//ReportAssignDrive::execute
//------------------------------------------------------------------------------  
  void  GlobalStatusReporter::ReportAssignDrive::
  execute(GlobalStatusReporter& reporter) {
    reporter.m_vdqmProxy.assignDrive(server,unitName, dgn, mountTransactionId,sessionPid);
  }
  
//------------------------------------------------------------------------------
// ReportGotDetailsFromClient::ReportGotDetailsFromClient
//------------------------------------------------------------------------------
  GlobalStatusReporter::
  ReportGotDetailsFromClient::ReportGotDetailsFromClient(const std::string &_unitName,
  const std::string &_vid):Report(_unitName),vid(_vid){}
//------------------------------------------------------------------------------
// ReportGotDetailsFromClient::execute
//------------------------------------------------------------------------------
    void GlobalStatusReporter::ReportGotDetailsFromClient::execute(
    GlobalStatusReporter& parent){
      parent.m_tapeserverProxy.gotWriteMountDetailsFromClient(unitName,vid);
    }
//------------------------------------------------------------------------------
// ReportTapeMountedForRead::ReportTapeMountedForRead
//------------------------------------------------------------------------------    
    GlobalStatusReporter::ReportTapeMountedForRead::
    ReportTapeMountedForRead(const std::string &server, 
           const std::string &unitName,const std::string &dgn, 
           const std::string &_vid, const pid_t sessionPid):
           Report(server,unitName,dgn,sessionPid),vid(_vid){
    
    }
//------------------------------------------------------------------------------
// ReportTapeMountedForRead::execute
//------------------------------------------------------------------------------        
    void GlobalStatusReporter::ReportTapeMountedForRead::
    execute(GlobalStatusReporter& parent){
      throw castor::tape::Exception("TODO ReportTapeMountedForRead::execute");
      parent.m_tapeserverProxy.tapeMountedForRead(unitName,vid);
      parent.m_vdqmProxy.tapeMounted(server,unitName,dgn,vid,sessionPid);
    }
    //------------------------------------------------------------------------------
// ReportTapeUnmounted::ReportTapeUnmounted
//------------------------------------------------------------------------------    
    GlobalStatusReporter::ReportTapeUnmounted::
    ReportTapeUnmounted(const std::string &server,const std::string &unitName, 
            const std::string &dgn,const std::string & _vid):
    Report(server,unitName,dgn),vid(_vid){
    
    }
//------------------------------------------------------------------------------
// ReportTapeUnmounted::execute
//------------------------------------------------------------------------------        
    void GlobalStatusReporter::ReportTapeUnmounted::
    execute(GlobalStatusReporter& parent){
      throw castor::tape::Exception("TODO ReportTapeUnmounted::execute");
      parent.m_tapeserverProxy.tapeUnmounted(unitName,vid);
      parent.m_vdqmProxy.tapeUnmounted(server,unitName,dgn,vid);
    }
}}}}



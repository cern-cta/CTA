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
//Constructor
//------------------------------------------------------------------------------  
  GlobalStatusReporter::GlobalStatusReporter(
  legacymsg::TapeserverProxy& tapeserverProxy,
  legacymsg::VdqmProxy& vdqmProxy,legacymsg::VmgrProxy& vmgrProxy,
          legacymsg::RmcProxy& rmcProxy,log::LogContext lc):
          m_tapeserverProxy(tapeserverProxy),m_vdqmProxy(vdqmProxy),
          m_vmgrProxy(vmgrProxy),m_rmcProxy(rmcProxy),m_lc(lc){
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
            const pid_t _sessionPid):server(_server),unitName(_unitName),
          dgn(_dgn),mountTransactionId(_mountTransactionId),sessionPid(_sessionPid)
  {}
//------------------------------------------------------------------------------
//ReportAssignDrive::execute
//------------------------------------------------------------------------------  
  void  GlobalStatusReporter::ReportAssignDrive::
  execute(GlobalStatusReporter& reporter)
  {
    reporter.m_vdqmProxy.assignDrive(server,unitName, dgn, mountTransactionId,sessionPid);
  }
  
//------------------------------------------------------------------------------
// ReportGotDetailsFromClient::ReportGotDetailsFromClient
//------------------------------------------------------------------------------
  GlobalStatusReporter::
  ReportGotDetailsFromClient::ReportGotDetailsFromClient(const std::string &_unitName,
  const std::string &_vid):unitName(_unitName),vid(_vid){}
//------------------------------------------------------------------------------
// ReportGotDetailsFromClient::execute
//------------------------------------------------------------------------------
    void GlobalStatusReporter::ReportGotDetailsFromClient::execute(
    GlobalStatusReporter& parent){
      parent.m_tapeserverProxy.gotWriteMountDetailsFromClient(unitName,vid);
    }
}}}}



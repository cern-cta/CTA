#include "castor/tape/tapeserver/daemon/GlobalStatusReporter.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/legacymsg/TapeserverProxy.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
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
//run
//------------------------------------------------------------------------------  
  void GlobalStatusReporter::run(){
    
    std::auto_ptr<Report> currentReport(m_fifo.pop());
    while(NULL!=currentReport.get()){
      currentReport->execute(*this);
    }
  }
  
  
}}}}



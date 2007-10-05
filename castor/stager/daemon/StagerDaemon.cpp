/**************************************************/
/* Main function to test the new StagerDBService */
/************************************************/
#include "castor/Constants.hpp"

#include "Cgetopt.h"

#include <errno.h>
#include "serrno.h"

#include "castor/exception/Exception.hpp"

#include "castor/stager/dbService/StagerMainDaemon.hpp"
#include "castor/stager/dbService/StagerDBService.hpp"

#include "castor/BaseObject.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/server/BaseDaemon.hpp"
#include "castor/server/BaseServer.hpp"

#include "castor/server/SelectProcessThread.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "castor/stager/dbService/JobRequestSvc.hpp"
#include "castor/stager/dbService/PreRequestSvc.hpp"
#include "castor/stager/dbService/StgRequestSvc.hpp"


#include <iostream>
#include <string>

/* size of the longopt (used for setting the configuration) */
#define LONGOPT_SIZE 17
#define STAGER_MSG_ERROR 1

int main(int argc, char* argv[]){
  try{

    /* initialize the error numbers */
    errno = serrno =0;

    castor::stager::dbService::StagerMainDaemon stgMainDaemon;
    

    /* we need to call this function before setting the number of threads */
  
    if(stgMainDaemon.stagerHelp){
      return(0);
    }
    
    if(putenv("RM_MAXRETRY=0") != 0){
      castor::exception::Exception ex(SEINTERNAL);
      ex.getMessage()<<"(StagerMainDaemon main) error on putenv(RM_MAXRETRY = 0)"<<std::endl;
      throw(ex);
    }

   
    
    /*****************************************/
    /* thread pools for the StagerDBService */
    /***************************************/
    castor::stager::dbService::JobRequestSvc* stgJob = new castor::stager::dbService::JobRequestSvc();
    castor::stager::dbService::PreRequestSvc* stgPre = new castor::stager::dbService::PreRequestSvc();
    castor::stager::dbService::StgRequestSvc* stgStg = new castor::stager::dbService::StgRequestSvc();
    
    stgMainDaemon.addThreadPool(new castor::server::SignalThreadPool("JobRequestSvc", stgJob));
    stgMainDaemon.addThreadPool(new castor::server::SignalThreadPool("PreRequestSvc", stgPre));
    stgMainDaemon.addThreadPool(new castor::server::SignalThreadPool("StgRequestSvc", stgStg));

    /* we need to call this function before setting the number of threads */
    stgMainDaemon.parseCommandLine(argc, argv);

    stgMainDaemon.getThreadPool('J')->setNbThreads(10);
    stgMainDaemon.getThreadPool('P')->setNbThreads(5);
    stgMainDaemon.getThreadPool('S')->setNbThreads(3);
    
    
    /******/
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message","added thread pools: JobDBSvc, ")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, STAGER_MSG_ERROR, 1, params);
    /******/ 

    stgMainDaemon.start();  

  }catch(castor::exception::Exception ex){
    std::cerr<<"(StagerMainDaemon main)"<<ex.getMessage().str() <<std::endl;
    // "Exception caught problem to start the daemon"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
       castor::dlf::Param("Precise Message", ex.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_MSG_ERROR, 2, params);
    return -1;
    
  }catch(...){
    std::cerr<<"(StagerMainDaemon main) Catched unknow exception"<<std::endl;
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message","Caught general exception in StagerMainDaemon")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_MSG_ERROR, 1, params);
    return -1;
  }
  
  return 0;
}// end main


namespace castor{
  namespace stager{
    namespace dbService{
      
      /******************************************************************************************/
      /* constructor: initiallizes the DLF logging and set the default value to its attributes */
      /****************************************************************************************/
      StagerMainDaemon::StagerMainDaemon() throw(castor::exception::Exception) : castor::server::BaseDaemon("Stager"){
	
	this->stagerHelp = false;
	
	castor::dlf::Message stagerMainMessages[]={
	  { STAGER_MSG_ERROR, "error"},
	  {-1, ""}
	};
	
        dlfInit(stagerMainMessages);
      }

      /**************************************************************/
      /* help method for the configuration (from the command line) */
      /****************************************************************/
      /* inline void StagerMainDaemon::help(std::string programName) */


    }/* end namespace dbService */
  }/* end namespace stager */
}/* end namespace castor */

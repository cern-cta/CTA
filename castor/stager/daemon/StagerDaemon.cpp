/**************************************************/
/* Main function to test the new StagerDBService */
/************************************************/
#include "castor/Constants.hpp"

#include "Cgetopt.h"


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
#include "castor/stager/dbService/StagerDBService.hpp"

#include <iostream>
#include <string>

/* size of the longopt (used for setting the configuration) */
#define LONGOPT_SIZE 17
#define STAGER_MSG_ERROR 1

int main(int argc, char* argv[]){
  try{
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
    
    stgMainDaemon.addThreadPool(new castor::server::SignalThreadPool("StagerDBService", new castor::stager::dbService::StagerDBService()));
    stgMainDaemon.getThreadPool('S')->setNbThreads(stgMainDaemon.stagerDbNbthread);

    /******/
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message","added thread pool")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_MSG_ERROR, 2, params);
    /******/

    stgMainDaemon.parseCommandLine(argc, argv);

    stgMainDaemon.start();  
  }catch(castor::exception::Exception ex){
    std::cerr<<"(StagerMainDaemon main)"<<ex.getMessage()<<std::endl;
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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_MSG_ERROR, 2, params);
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
      StagerMainDaemon::StagerMainDaemon() throw(castor::exception::Exception) : castor::server::BaseDaemon("StagerMainDaemon"){      
	
	this->stagerHelp = false;
	this->stagerDbNbthread = 1;
	/* initialite the DLF login */
	castor::BaseObject::initLog("Stager", castor::SVC_STDMSG);
	
	castor::dlf::Message stagerMainMessages[]={
	  { STAGER_MSG_ERR, "error"},
	  {-1, ""}
	};
	castor::dlf::dlf_init("Stager", stagerMainMessages);
	
      }


      /********************************************************/
      /* parse command line(we overwrite the BaseDaemon one) */
      /******************************************************/
      /*  void StagerMainDaemon::parseCommandLine(int argc, char* argv[]) throw(castor::exception::Exception)*/ 
      
      /**************************************************************/
      /* help method for the configuration (from the command line) */
      /****************************************************************/
      /* inline void StagerMainDaemon::help(std::string programName) */


    }/* end namespace dbService */
  }/* end namespace stager */
}/* end namespace castor */

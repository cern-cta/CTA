/**************************************************/
/* Main function to test the new StagerDBService */
/************************************************/
#include "castor/Constants.hpp"
#include "stager_constants.h"
#include "stage_constants.h"
#include "Cgetopt.h"
#include "stager_admin_api.h"
#include "stager_messages.h"


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


int main(int argc, char* argv[]){
  try{
    castor::stager::dbService::StagerMainDaemon stgMainDaemon;
    
    /* we need to call this function before setting the number of threads */
    stgMainDaemon.parseCommandLine(argc, argv);
#ifndef CSEC
    if(stgMainDaemon.optionStagerSecure){
      return(0);
    }
#endif
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
	
	/* initialite the DLF login */
	castor::BaseObject::initLog("StagerMainDaemon", castor::SVC_STDMSG);
	
	castor::dlf::Message stagerMainMessages[]={
	  { STAGER_MSG_EMERGENCY, "emergency"},
	  { STAGER_MSG_ALERT, "alert"},
	  { STAGER_MSG_ERR, "error"},
	  { STAGER_MSG_SYSCALL, "sytem call error"},
	  { STAGER_MSG_WARNING, "authentication"},
	  { STAGER_MSG_SECURITY, "security"},
	  { STAGER_MSG_USAGE, "usage"},
	  { STAGER_MSG_ENTER, "function enter"},
	  { STAGER_MSG_LEAVE, "function leave"},
	  { STAGER_MSG_RETURN, "function return"},
	  { STAGER_MSG_SYSTEM, "system"},
	  { STAGER_MSG_STARTUP, "stager server started"},
	  { STAGER_MSG_IMPORTANT, "important"},
	  { STAGER_MSG_DEBUG, "debug"},
	  { STAGER_MSG_EXIT, "stager server exiting"},
	  { STAGER_MSG_RESTART, "stager server restart"},
	  {-1, ""}
	};
	castor::dlf::dlf_init("stager", stagerMainMessages);

	/* set the default value to its attributes */
	stagerHelp = 0;                      /* Stager help */
	stagerForeground = 0;                /* Stager foreground mode */

	stagerFacility.resize(CA_MAXLINELEN+1);
	stagerFacility.clear();
	stagerLog.resize(CA_MAXLINELEN+1);
	stagerLog.clear();
	char tmpStagerLog[CA_MAXLINELEN+1];
	char tmpStagerFacility[CA_MAXLINELEN+1];
	if(stager_configTimeout(&stagerTimeout) == 0 &&                   
	   stager_configSecurePort(&stagerSecurePort) == 0 &&             
	   stager_configSecure(&stagerSecure) == 0 &&                    
	   stager_configPort(&stagerPort) == 0 &&                          
	   stager_configDebug(&stagerDebug) == 0 &&                        
	   stager_configTrace(&stagerTrace ) == 0 &&                       
	   stager_configDbNbthread(&stagerDbNbthread) == 0 &&             
	   stager_configGCNbthread(&stagerGCNbthread) == 0 &&             
	   stager_configErrorNbthread(&stagerErrorNbthread) == 0 &&        
	   stager_configQueryNbthread(&stagerQueryNbthread) == 0 &&        
	   stager_configGetnextNbthread(&stagerGetNextNbthread) == 0 &&    
	   stager_configJobNbthread(&stagerAdminNbthread) == 0 &&          
	   stager_configAdminNbthread(&stagerAdminNbthread) == 0 &&        
	   stager_configFacility(CA_MAXLINELEN, tmpStagerFacility) == 0 &&    
	   stager_configLog(CA_MAXLINELEN, tmpStagerLog) == 0 &&              
	   stager_configIgnoreCommandLine(&stagerIgnoreCommandLine) == 0 &&
	   stager_configNoDlf(&stagerNoDlf) == 0){
	  stagerLog = tmpStagerLog;
	  stagerFacility = tmpStagerFacility;
	}else{
	  castor::exception::Exception ex(EINVAL);
	  ex.getMessage()<<"(StagerMainDaemon constructor) error while getting the configuration"<<std::endl;
	  throw(ex);
	}
	
      }

      /********************************************************/
      /* parse command line(we overwrite the BaseDaemon one) */
      /******************************************************/
      void StagerMainDaemon::parseCommandLine(int argc, char* argv[]) throw(castor::exception::Exception){
	
	
	/* stager.c : Command line parsing arguments: To switch the Cgetopt output */
	static struct Coptions longopts[] =
	  {
	    {"Adminthreads",       REQUIRED_ARGUMENT,  NULL,                 'A'},
	    {"Cthreads",           REQUIRED_ARGUMENT,  NULL,                 'C'},
	    {"debug",              NO_ARGUMENT,        NULL,                 'd'},
	    {"Dbthreads",          REQUIRED_ARGUMENT,  NULL,                 'D'},
	    {"foreground",         NO_ARGUMENT,        NULL,                 'f'},
	    {"Ethreads",           REQUIRED_ARGUMENT,  NULL,                 'E'},
	    /* {"Fsupdate",           REQUIRED_ARGUMENT,  NULL,                 'F'}, */
	    {"Gthreads",           REQUIRED_ARGUMENT,  NULL,                 'G'},
	    {"help",               NO_ARGUMENT,        NULL,                 'h'},
	    {"Jthreads",           REQUIRED_ARGUMENT,  NULL,                 'J'},
	    {"log",                REQUIRED_ARGUMENT,  NULL,                 'l'},
	    {"nodlf",              NO_ARGUMENT,        NULL,                 'n'},
	    {"Port",               REQUIRED_ARGUMENT,  NULL,                 'P'},
	    {"port",               REQUIRED_ARGUMENT,  NULL,                 'p'},
	    {"Qthreads",           REQUIRED_ARGUMENT,  NULL,                 'Q'},
	    {"Secure",             NO_ARGUMENT,        NULL,                 'S'},
	    {"trace",              NO_ARGUMENT,        NULL,                 't'},
	    {"Timeout",            REQUIRED_ARGUMENT,  NULL,                 'T'},
	    /*  {"ZFsthreads",         REQUIRED_ARGUMENT,  NULL,                 'Z'}, */
	    {NULL,                 0,                  NULL,                   0}
	  };

	/* BaseServer: */
	Coptind = 1;
	Copterr = 0;

	/* stager.c : print the main arguments */
	size_t sizeConcatenatedArgv = 0;
	if(argc > 0){
	  for(int c = 0; c<argc; c++){
	    sizeConcatenatedArgv += strlen(argv[c]);
	  }
	}
	if(sizeConcatenatedArgv <= 0){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerMainDaemon parseCommandLine) error on the command line arguments"<<std::endl;
	  throw(ex);
	}
	sizeConcatenatedArgv += argc;

	/* to print the main arguments */
	char* stgMainArguments = (char*) calloc(1,sizeConcatenatedArgv);
	if(stgMainArguments == NULL){
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerMainDaemon parseCommandLine) error while reserving memory"<<std::endl;
	  throw(ex);
	}

	
	for(int c = 0; c < argc; c++){
	  if(c > 0 ){
	    *stgMainArguments++ = ' ';
	  }
	  sprintf(stgMainArguments, "%s", argv[c]);
	}

	/* take the configuration from the command line (if it is needed) */
	

	/* stager.c : parse the command line */
	int c = 0;
	Coptarg = NULL; 

	bool optionStagerSecure = false;
	
	while (c = Cgetopt_long(argc,argv,"A:C:dD:fE:F:G:hJ:l:np:P:Q:StT:Z:", longopts, NULL) != -1) {
	  switch (c) {
	  case 'A':
	    stagerAdminNbthread = stagerIgnoreCommandLine == true? stagerAdminNbthread : atoi(Coptarg);
	    break;
	  case 'd':
	    stagerDebug = stagerIgnoreCommandLine == true? stagerDebug : 1;
	    /* Option -d implies option -t */
	    stagerTrace = stagerIgnoreCommandLine == true? stagerTrace : 1;
	    break;
	  case 'C':
	    stagerGCNbthread = stagerIgnoreCommandLine == true? stagerGCNbthread : atoi(Coptarg);
	    break;
	  case 'D':
	    stagerDbNbthread = stagerIgnoreCommandLine == true? stagerDbNbthread : atoi(Coptarg);
	    break;
	  case 'E':
	    stagerErrorNbthread = stagerIgnoreCommandLine == true? stagerErrorNbthread : atoi(Coptarg);
	    break;
	  case 'f':
	    stagerForeground = stagerIgnoreCommandLine == true? stagerForeground : 1;
	    break;
	    /* case 'F':
	       stagerFsUpdate = atoi(Coptarg);
	       break; */
	  case 'G':
	    stagerGetNextNbthread = stagerIgnoreCommandLine == true? stagerGetNextNbthread : atoi(Coptarg);
	    break;
	  case 'h':
	    stagerHelp =stagerIgnoreCommandLine == true? stagerHelp : 1;
	    break;
	  case 'J':
	    stagerJobNbthread = stagerIgnoreCommandLine == true? stagerJobNbthread : atoi(Coptarg);
	    break;
	  case 'l':
	    if(strlen(Coptarg) > CA_MAXLINELEN){
	      castor::exception::Exception ex(SENAMETOOLONG);
	      ex.getMessage()<<"(StagerMainDaemon parseCommandLine) Coptarg too long"<<std::endl;
	      throw(ex); 
	    }
	    if(Coptarg[0] == '\0'){
	      castor::exception::Exception ex(SEINTERNAL);
	      ex.getMessage()<<"(StagerMainDaemon parseCommandLine) Coptarg invalid"<<std::endl;
	      throw(ex); 
	    }
	    stagerLog = stagerIgnoreCommandLine == true? stagerLog : Coptarg;
	    break;
	  case 'n':
	    stagerNoDlf = stagerIgnoreCommandLine == true? stagerNoDlf : 1;
	  case 'p':
	    stagerPort = stagerIgnoreCommandLine == true? stagerPort : atoi(Coptarg);
	    if (stagerPort <= 0) {
	      castor::exception::Exception ex(EINVAL);
	      ex.getMessage()<<"(StagerMainDaemon parseCommandLine) SecurePort must be >0"<<std::endl;
	      throw(ex);
	    }
	    break;
	  case 'P':
	    stagerSecurePort = stagerIgnoreCommandLine == true? stagerSecurePort : atoi(Coptarg);
	    if (stagerSecurePort <= 0) {
	      castor::exception::Exception ex(EINVAL);
	      ex.getMessage()<<"(StagerMainDaemon parseCommandLine) SecurePort must be >0"<<std::endl;
	      throw(ex);
	    }
	    break;
	  case 'Q':
	    stagerQueryNbthread = stagerIgnoreCommandLine == true? stagerQueryNbthread : atoi(Coptarg);
	    break;
	  case 'S':
	    stagerSecure = stagerIgnoreCommandLine == true? stagerSecure : 1;
	    optionStagerSecure = true; 
	    break;
	  case 't':
	    stagerTrace = stagerIgnoreCommandLine == true? stagerTrace : 1;
	    break;
	  case 'T':
	    stagerTimeout = stagerIgnoreCommandLine == true? stagerTimeout : atoi(Coptarg);
	    break;
	    /* case 'Z':
	       stagerFsNbthread = atoi(Coptarg);
	       break; */
	  default:
	    std::string mainArgs = argv[0];
	    help(mainArgs);
	    castor::exception::Exception ex(EINVAL);
	    ex.getMessage()<<"(StagerMainDaemon parseCommandLine) invalid argument on the command line"<<std::endl;
	    throw(ex);
	    break;
	  }
	}
      
      
#ifndef CSEC
      if(optionStagerSecure){
	castor::dlf::Param param[]={castor::dlf::Param("Standard Message","Stager not compiled with security, but started with security option")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE,STAGER_MSG_USAGE, 1, param);
	help(argv[0]);
      }
#endif	
      
      /* if(stagerLog.empty() == false){
	if(strcmp(stagerLog.c_str(), "stderr")== 0 ){
	   castor::dlf::Param params[] =
	     {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
	      castor::dlf::Param("Precise Message", ex.getMessage().str())};
	   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_MSG_ERROR, 2, params);
	  initlog("stager",LOG_DEBUG,"");	    
	}else{
	  initlog("stager", LOG_DEBUG,(char*)stagerLog.c_str());
	}
	}*/
       
      if(stagerHelp){
	std::string mainArgs = argv[0];
	help(mainArgs);
      }
	  
      free(stgMainArguments);
      }

      
      /**************************************************************/
      /* help method for the configuration (from the command line) */
      /****************************************************************/
      /* inline void StagerMainDaemon::help(std::string programName) */


    }/* end namespace dbService */
  }/* end namespace stager */
}/* end namespace castor */

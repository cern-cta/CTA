/**************************************************/
/* Main function to test the new StagerDBService */
/************************************************/
#ifndef STAGER_MAIN_DAEMON_HPP
#define STAGER_MAIN_DAEMON_HPP 1


#include "castor/server/BaseDaemon.hpp"

namespace castor{
  namespace stager{
    namespace dbService{


      class StagerMainDaemon : public castor::server::BaseDaemon{

      public:
	/*** constructor ***/
	StagerMainDaemon() throw(castor::exception::Exception);
	/*** destructor ***/
	virtual ~StagerMainDaemon() throw() {};
	/**********************************************************************************/
	/* looped switch to get and set the configuration values (from the command line) */
	/********************************************************************************/
	bool StagerMainDaemon::switchSetConfig(int argc, char* argv[]) throw(castor::exception::Exception);

     
	/* option for the configuration(from the command line) */
	int stagerHelp;                      /* Stager help */
	int stagerForeground;                /* Stager foreground mode */
	int stagerTimeout;                  /* Stager network timeout */
	int stagerSecurePort;               /* Stager secure port number */
	int stagerSecure;                   /* Stager secure mode */
	int optionStagerSecure;
	int stagerPort;                     /* Stager port number */
	int stagerDebug;
	int stagerTrace;
	int stagerDbNbthread;                /* Stager number of db threads in the db pool */
	int stagerGCNbthread;             /* Stager number of query threads in the query pool */
	int stagerErrorNbthread;           /* Stager number of getnext threads in the getnext pool */
	int stagerQueryNbthread;             /* Stager number of admin threads */
	int stagerGetNextNbthread;               /* Stager number of job threads (beware, we open a port) */
	int stagerJobNbthread;                /* Stager number of Garbage Collector threads in the GC pool */
	int stagerAdminNbthread;             /* Stager number of Error threads in the Error pool */
	std::string stagerFacility;    /* Stager facility (for logging in DLF) */
	std::string stagerLog;
	int stagerIgnoreCommandLine;         /* Stager ignore-command-line mode */
	int stagerNoDlf;

      }; /* end class StagerMainDaemon */
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor




#endif

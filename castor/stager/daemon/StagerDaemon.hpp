/**************************************************/
/* Main function to test the new StagerDBService */
/************************************************/
#ifndef STAGER_MAIN_DAEMON_HPP
#define STAGER_MAIN_DAEMON_HPP 1


#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{


      class StagerMainDaemon : public castor::server::BaseDaemon{

      public:
	/*** constructor ***/
	StagerMainDaemon() throw(castor::exception::Exception);
	/*** destructor ***/
	virtual ~StagerMainDaemon() throw() {};






	/*********************************************************************************/
	/* BaseServer::parseCommandLine parses a command line to set the server options */
	/*******************************************************************************/
	void parseCommandLine(int argc, char* argv[]) throw(castor::exception::Exception);



	/*****************************************************************/
	/* BaseServer::help print the valid options for the commandLine */
	/***************************************************************/
	inline void help(std::string programName){
	  std::cout << "Usage: " << programName << " [options]\n"
	    "\n"
	    "where options can be:\n"
	    "\n"
	    "\t--Adminthreads or -A {integer >= 0} \tNumber of admin threads (should be 1 or zero)\n"
	    "\t--debug        or -d                \tDebug mode\n"
	    "\t--Cthreads     or -C {integer >= 0}  \tNumber of garbage collector threads\n"
	    "\t--Dbthreads    or -D {integer >= 0}  \tNumber of threads to the database\n"
	    "\t--Ethreads     or -E {integer >= 0}  \tNumber of error threads\n"
	    "\t--foreground   or -f                \tForeground\n"
	    "\t--Gthreads     or -G {integer >= 0}  \tNumber of getnext threads\n"
	    "\t--help         or -h                \tThis help\n"
	    "\t--Jthreads     or -J {integer >= 0}  \tNumber of job threads\n"
	    "\t--log          or -l {string}       \tForced logging outside of DLF (filename, or stderr, or stdout)\n"
	    "\t--nodlf        or -n                \tForced no logging to DLF (then consider option --log)\n"
	    "\t--port         or -p {integer > 0}  \tPort number to listen\n"
	    "\t--Port         or -P {integer > 0}  \tSecure Port number to listen\n"
	    "\t--Qthreads     or -Q {integer >= 0}  \tNumber of query threads\n"
	    "\t--Secure       or -S                \tSecure mode\n"
	    "\t--trace        or -t                \tTrace mode\n"
	    "\t--Timeout      or -T {integer}      \tNetwork timeout\n"
	    "\n"
#ifndef CSEC
	    "WARNING: Option -P will be refused\n"
	    "Please re-compile stager with security support (-DCSEC)\n"
	    "\n"
#endif
	    "\n"
	    "Please note that option -d implies option -t\n"
	    "\n"
	    "Comments to: Castor.Support@cern.ch\n";
	}

	

     
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

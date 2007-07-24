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


	/*****************************************************************/
	/* BaseServer::help print the valid options for the commandLine */
	/***************************************************************/
	inline void help(std::string programName){
	  std::cout << "Usage: " << programName << " [options]\n"
	    "\n"
	    "where options can be:\n"
	    "\n"
	    "\t--Dbthreads    or -D {integer >= 0}  \tNumber of threads to the database\n"
	    "\t--log          or -l {string}       \tForced logging outside of DLF (filename, or stderr, or stdout)\n"
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

	int stagerDbNbthread;                /* Stager number of db threads in the db pool */
	std::string stagerLog;
	bool stagerHelp;

      }; /* end class StagerMainDaemon */
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor




#endif

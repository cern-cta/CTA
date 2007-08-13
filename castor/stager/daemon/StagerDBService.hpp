
/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#ifndef STAGER_DB_SERVICE_HPP
#define STAGER_DB_SERVICE_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/Constants.hpp"


#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"

#include <string>
#include <iostream>

namespace castor {
  namespace stager{
    namespace dbService {
      
      class StagerRequestHelper;
      class StagerCnsHelper;
    
      class StagerDBService : public virtual castor::server::SelectProcessThread{
	
      private:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	
	
	
      public: 
	/* constructor */
	StagerDBService() throw();
	~StagerDBService() throw();

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual castor::IObject* select() throw(castor::exception::Exception);
	virtual void process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception);



	/* coming from the latest stager_db_service.c */
	/* --------------------------------------------------- */
	/* sendNotification()                                  */
	/*                                                     */
	/* Send a notification message to another CASTOR2      */
	/* daemon using the NotificationThread model. This     */
	/* will wake up the process and trigger it to perform  */
	/* a dedicated action.                                 */
	/*                                                     */
	/* This function is copied from BaseServer.cpp. We     */
	/* could have wrapped the C++ call to be callable in   */
	/* C. However, as the stager is being re-written in    */
	/* C++ anyway we take this shortcut.                   */
	/*                                                     */
	/* Input:  (const char *) host - host to notify        */
	/*         (const int) port - notification por         */
	/*         (const int) nbThreads - number of threads   */
	/*                      to wake up on the server       */
	/*                                                     */
	/* Return: nothing (void). All errors ignored          */
	/* --------------------------------------------------- */
	void sendNotification(const char *host, const int port, const int nbThreads);
	
	std::vector<ObjectsIds> types;
	
	
      };// end class StagerDBService
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif

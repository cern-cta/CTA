/*******************************************************************************************************/
/* Base class for StagerJobRequestHandler and all the fileRequest handlers                            */
/* Basically: handle() as METHOD  and  (stgRequestHelper,stgCnsHelper,stgReplyHelper)  as ATTRIBUTES */
/****************************************************************************************************/

#ifndef STAGER_REQUEST_HANDLER_HPP
#define STAGER_REQUEST_HANDLER_HPP 1

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"


#include "../../exception/Exception.hpp"
#include "../../IObject.hpp"
#include "../../ObjectSet.hpp"


#include <string>
#include <iostream>

#define DEFAULTFILESIZE (2 * (u_signed64) 1000000000)


namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;
      class StagerReplyHelper;

      class StagerRequestHandler : public virtual castor::IObject{
      public:
	/* empty destructor */
	virtual StagerRequestHandler::~StagerRequestHandler() throw();
	
	/* main function for the specific request handler */
	virtual void StagerRequestHandler::handle() throw (castor::exception::Exception) const;

	/*********************************************/
	/* virtual functions inherited from IOBject */
	/*******************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;
 
      protected:
	StagerRequestHelper *stgRequestHelper;
	StagerCnsHelper *stgCnsHelper;
	StagerReplyHelper *stgReplyHelper;

     

      };/* end StagerRequestHandler class */

    }
  }
}




#endif


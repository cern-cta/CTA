/*******************************************************************************************************/
/* Base class for StagerJobRequestHandler and all the fileRequest handlers                            */
/* Basically: handle() as METHOD  and  (stgRequestHelper,stgCnsHelper,stgReplyHelper)  as ATTRIBUTES */
/****************************************************************************************************/

#ifndef STAGER_REQUEST_HANDLER_HPP
#define STAGER_REQUEST_HANDLER_HPP 1

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"


#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"


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
	virtual ~StagerRequestHandler() throw() {};
	
	/* main function for the specific request handler */
	virtual void handle() throw (castor::exception::Exception) = 0;

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


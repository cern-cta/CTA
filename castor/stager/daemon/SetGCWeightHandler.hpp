/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/


#ifndef STAGER_SET_GC_HANDLER_HPP
#define STAGER_SET_GC_HANDLER_HPP 1

#include "StagerRequestHandler.hpp"

#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "../../../h/u64subr.h"

#include "../SetFileGCWeight.hpp"

#include "../../IObject.hpp"
#include "../../ObjectSet.hpp"
#include "../../exception/Exception.hpp"

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;
      class SetFileGCWeight;

      class StagerSetGCHandler : public virtual StagerRequestHandler{

     
      public:
	/* constructor */
	StagerSetGCHandler::StagerSetGCHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	StagerSetGCHandler::~StagerSetGCHandler() throw();

	/* SetFileGCWeight handle implementation */
	void StagerSetGCHandler::handle() throw(castor::exception::Exception);

	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;


      private:
	castor::stager::SetFileGCWeight* setFileGCWeight;
     
      }; //end StagerSetGCHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor

#endif 

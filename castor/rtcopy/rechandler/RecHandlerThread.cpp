/******************************************************************************
 *                      RecHandlerThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: RecHandlerThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/rtcopy/rechandler/RecHandlerThread.hpp"


#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/infoPolicy/IPolicySvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/infoPolicy/DbInfoRecallPolicy.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include "castor/infoPolicy/RecallPySvc.hpp" 


#include <Cns_api.h>
#include <u64subr.h>

#include "castor/infoPolicy/RecallPySvc.hpp"

#include "castor/infoPolicy/DbInfoRecallPolicy.hpp"
#include "castor/infoPolicy/DbInfoPolicy.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"



namespace castor {

  namespace rtcopy {

    namespace rechandler {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RecHandlerThread::RecHandlerThread(castor::infoPolicy::IPolicySvc* mySvc, std::vector<std::string> listSvcClass,castor::infoPolicy::RecallPySvc* recallPolicy) { 
   m_listSvcClass=listSvcClass;  
   m_policySvc=mySvc;
   m_recallPolicy=recallPolicy;

}


//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void RecHandlerThread::run(void* par)
{
  std::vector<castor::infoPolicy::PolicyObj*>::iterator infoCandidate;
  std::vector<u_signed64> eligibleTapeIds; // output with Id to resurrect
  std::vector<castor::infoPolicy::PolicyObj*> infoCandidateTape;
  castor::infoPolicy::DbInfoRecallPolicy* realInfo=NULL;
  
  try{

    // loop all the given  svc class 
    std::vector<std::string>::iterator svcClassName=m_listSvcClass.begin();
    while(svcClassName != m_listSvcClass.end()){  

      // get information from the db    
      infoCandidateTape = m_policySvc->inputForRecallPolicy(*svcClassName);
      
      if (infoCandidateTape.empty()) {
	 castor::dlf::Param params[] =
         {castor::dlf::Param("SvcClass", (*svcClassName)),
          castor::dlf::Param("message", "No input tapes for the policy")};
         castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 3, 2, params);

      }

      // initialize the pysvc with the policy name

      infoCandidate=infoCandidateTape.begin();
      
      while ( infoCandidate != infoCandidateTape.end() ){
        // call the policy for each one
	if (!((*infoCandidate)->dbInfoPolicy().empty()))
          realInfo=dynamic_cast<castor::infoPolicy::DbInfoRecallPolicy*>((*infoCandidate)->dbInfoPolicy()[0]);
        if (realInfo == NULL){
	      castor::dlf::Param params[] =
	      {castor::dlf::Param("message", "Error in getting the tape information")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
	      continue;
	}

        if (m_recallPolicy != NULL){
	  castor::dlf::Param params[] =
	      {castor::dlf::Param("Tape", realInfo->vid()),
	       castor::dlf::Param("message", "Policy called")};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 4, 2, params);
	}

	try {
	  if ( m_recallPolicy == NULL || 
	       m_recallPolicy->applyPolicy(*infoCandidate)){

	    eligibleTapeIds.push_back(realInfo->tapeId()); // tape to resurrect
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("Tape", realInfo->vid())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 5, 1, params);

	  }
	} catch (castor::exception::Exception e) {
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("code", sstrerror(e.code())),
	     castor::dlf::Param("message", e.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 10, 2, params);
	  // I allow the recall
	  if (realInfo)
	    eligibleTapeIds.push_back(realInfo->tapeId());
 
	    castor::dlf::Param params2[] =
	      {castor::dlf::Param("Tape", realInfo->vid())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 5, 1, params2);
	}

	

	// delete the object

	 if (*infoCandidate){
	   delete *infoCandidate;
	   *infoCandidate=NULL;

	   if (realInfo) delete realInfo;
	   realInfo=NULL;

	 }

         infoCandidate++;
      }

      // call the db to put the tape as pending for that svcclass
      if (eligibleTapeIds.empty()){
	 castor::dlf::Param params[] =
	      {castor::dlf::Param("SvcCLass",*svcClassName),
	       castor::dlf::Param("message", "No eligible tape to resurrect")};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 5, 2, params);
	
      }

      m_policySvc->resurrectTapes(eligibleTapeIds);
      svcClassName++;
    }

  }
  catch(castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("code", sstrerror(e.code())),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 2, params);
    
    // CLEAN UP
     
     infoCandidate=infoCandidateTape.begin();
     while ( infoCandidate != infoCandidateTape.end() ){
        if (*infoCandidate){
	  if (!((*infoCandidate)->dbInfoPolicy().empty()))
	    realInfo=dynamic_cast<castor::infoPolicy::DbInfoRecallPolicy*>((*infoCandidate)->dbInfoPolicy()[0]);
	  
	  delete *infoCandidate;
	  *infoCandidate=NULL;

	  if (realInfo) delete realInfo;
	  realInfo=NULL;

	}
        infoCandidate++;
     }

  
  }
  catch (...) {
    castor::dlf::Param params2[] =
      {castor::dlf::Param("message", "general exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params2);
    
    // CLEAN UP 

     infoCandidate=infoCandidateTape.begin();
     while ( infoCandidate != infoCandidateTape.end() ){
        if (*infoCandidate){
	  if (!((*infoCandidate)->dbInfoPolicy().empty()))
	    realInfo=dynamic_cast<castor::infoPolicy::DbInfoRecallPolicy*>((*infoCandidate)->dbInfoPolicy()[0]);

	  delete *infoCandidate;
	  *infoCandidate=NULL;
	  if (realInfo) delete realInfo;
	  realInfo=NULL;
	  delete *infoCandidate;
	  *infoCandidate=NULL;
	}
        infoCandidate++;
     }

  }
}   

  
  }  // END Namespace rechandler
 }  // END Namespace rtcopy
}  // END Namespace castor

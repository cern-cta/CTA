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
 * @(#)$RCSfile: RecHandlerThread.cpp,v $ $Author: waldron $
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

#include "castor/exception/Exception.hpp"
#include "castor/infoPolicy/RecallPolicyElement.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include "castor/infoPolicy/RecallPySvc.hpp" 


#include <Cns_api.h>
#include <u64subr.h>

#include "castor/rtcopy/rechandler/IRecHandlerSvc.hpp"

#include "castor/infoPolicy/RecallPolicyElement.hpp"

#include "castor/rtcopy/rechandler/DlfCodes.hpp"
#include "castor/rtcopy/rechandler/IRecHandlerSvc.hpp"

#include <list>

// to implement the priority hack

extern "C" {
  int vdqm_SendVolPriority(char*, int, int, int);
};

namespace castor {

  namespace rtcopy {

    namespace rechandler {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RecHandlerThread::RecHandlerThread(castor::infoPolicy::RecallPySvc* recallPolicy) { 
   m_recallPolicy=recallPolicy;

}


//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void RecHandlerThread::run(void* par)
{

  //get  the db svc

  castor::IService* dbSvc = castor::BaseObject::services()->service("OraRecHandlerSvc", castor::SVC_ORARECHANDLERSVC);
  castor::rtcopy::rechandler::IRecHandlerSvc* oraSvc = dynamic_cast<castor::rtcopy::rechandler::IRecHandlerSvc*>(dbSvc);
  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,FATAL_ERROR, 0, NULL);
    return;
  }

  std::list<castor::infoPolicy::RecallPolicyElement>::iterator infoCandidate;
  std::list<u_signed64> eligibleTapeIds; // output with Id to resurrect
  std::list<castor::infoPolicy::RecallPolicyElement> infoCandidateTape;
 
  
  try{

      
    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);



    // get information from the db    
    oraSvc->inputForRecallPolicy(infoCandidateTape);
    
    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    castor::dlf::Param paramsDb[] =
      {
	    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
    
    
    if (infoCandidateTape.empty()) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPE_NOT_FOUND, 1, paramsDb);
      return;
    }

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPE_FOUND, 1, paramsDb);

    // initialize the pysvc with the policy name

    infoCandidate=infoCandidateTape.begin();

    while ( infoCandidate != infoCandidateTape.end() ){
      // call the policy for each one	
      castor::dlf::Param paramsInput[] =
	{
	 castor::dlf::Param("TPVID", (*infoCandidate).vid()),
	 castor::dlf::Param("numFiles", (*infoCandidate).numFiles()),
	 castor::dlf::Param("numBytes", (*infoCandidate).numBytes()),
	 castor::dlf::Param("age", (*infoCandidate).oldest()),
	 castor::dlf::Param("priority", (*infoCandidate).priority())  
	};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, POLICY_INPUT,5, paramsInput);


      try {
	if ( m_recallPolicy == NULL ) {
	  //no priority sent

	  eligibleTapeIds.push_back((*infoCandidate).tapeId()); // tape to resurrect
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("TPVID", (*infoCandidate).vid())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,ALLOWED_WITHOUT_POLICY , 1, params);

	} else {
	  int priorityChosen=m_recallPolicy->applyPolicy(&(*infoCandidate));
	  
	  if (priorityChosen >= 0) {
	    eligibleTapeIds.push_back((*infoCandidate).tapeId()); // tape to resurrect
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("TPVID", (*infoCandidate).vid())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, ALLOWED_BY_POLICY, 1, params);
	      
	    // call to VDQM with the priority (temporary hack)
	    gettimeofday(&tvStart, NULL);
	    
	    vdqm_SendVolPriority((char*)(*infoCandidate).vid().c_str(),0,priorityChosen,0);
	    
	    gettimeofday(&tvEnd, NULL);
	    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
	    castor::dlf::Param paramsVdqm[] =
	      {
		castor::dlf::Param("TPVID",(*infoCandidate).vid()),
		castor::dlf::Param("ProcessingTime", procTime * 0.000001),
		castor::dlf::Param("Priority", priorityChosen)
	      };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRIORITY_SENT,3, paramsVdqm);
	     
     



	  } else {
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("TPVID", (*infoCandidate).vid())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, NOT_ALLOWED, 1, params);
	  }
	}
      } catch (castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {castor::dlf::Param("code", sstrerror(e.code())),
	   castor::dlf::Param("message", e.getMessage().str())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PYTHON_ERROR , 2, params);
	// I allow the recall

	eligibleTapeIds.push_back((*infoCandidate).tapeId());
 
	castor::dlf::Param params2[] =
	  {castor::dlf::Param("TPVID", (*infoCandidate).vid())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, ALLOWED_WITHOUT_POLICY, 1, params2);
      }

      infoCandidate++;

    }

    // call the db to put the tape as pending for that svcclass
    if (!eligibleTapeIds.empty()){

      gettimeofday(&tvStart, NULL);
      oraSvc->resurrectTapes(eligibleTapeIds);
      
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
      castor::dlf::Param paramsDb2[] =
	{
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, RESURRECT_TAPES, 1, paramsDb2);
	     

    }

  } catch(castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("code", sstrerror(e.code())),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 2, params);
    
  } catch (...) {
    castor::dlf::Param params2[] =
      {castor::dlf::Param("message", "general exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 1, params2);
    
  }
  
 
  eligibleTapeIds.clear();
  infoCandidateTape.clear();

}   

  
  }  // END Namespace rechandler
 }  // END Namespace rtcopy
}  // END Namespace castor

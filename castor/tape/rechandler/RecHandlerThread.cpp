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

// first because of python.h issues


// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include "castor/tape/python/python.hpp"

#include <Cns_api.h>
#include <list>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/tape/rechandler/IRecHandlerSvc.hpp"
#include "castor/tape/rechandler/RecallPolicyElement.hpp"
#include "castor/tape/rechandler/RecHandlerDlfMessageConstants.hpp" 
#include "castor/tape/rechandler/RecHandlerThread.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"

#include "castor/stager/TapeStatusCodes.hpp"

// to implement the priority hack

extern "C" {
  int vdqm_SendVolPriority(char*, int, int, int);
};


  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rechandler::RecHandlerThread::RecHandlerThread(PyObject* pyFunction){
  m_pyFunction=pyFunction;
}


//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::rechandler::RecHandlerThread::run(void*)
{

  //get  the db svc

  castor::IService* dbSvc = castor::BaseObject::services()->service("OraRecHandlerSvc", castor::SVC_ORARECHANDLERSVC);
  castor::tape::rechandler::IRecHandlerSvc* oraSvc = dynamic_cast<castor::tape::rechandler::IRecHandlerSvc*>(dbSvc);
  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,FATAL_ERROR, 0, NULL);
    return;
  }

  std::list<castor::tape::rechandler::RecallPolicyElement>::iterator infoCandidate;
  std::list<u_signed64> eligibleTapeIds; // output with Id to resurrect
  std::list<castor::tape::rechandler::RecallPolicyElement> infoCandidateTape;

  try{
      
    timeval tvStart,tvEnd;
    gettimeofday(&tvStart, NULL);

    // get information from the db   
    int nbMountsForRecall=0; 
    oraSvc->tapesAndMountsForRecallPolicy(infoCandidateTape,nbMountsForRecall);
    
    gettimeofday(&tvEnd, NULL);
    signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
    castor::dlf::Param paramsDb[] =
      {
	    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
    
    if (infoCandidateTape.empty()) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPE_NOT_FOUND, paramsDb);
      return;
    }

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPE_FOUND, paramsDb);
     
    infoCandidate=infoCandidateTape.begin();

    while ( infoCandidate != infoCandidateTape.end() ){
      
      bool tagForRecallAllowed=true;
      
      if ( castor::stager::TAPE_WAITPOLICY == infoCandidate->status ) { // tape in WAITPOLICY state
        // here we only allow or disallow a recall from the tape
        try {
          if ( m_pyFunction == NULL ) {  //no priority sent
            eligibleTapeIds.push_back(infoCandidate->tapeId); // tape to resurrect
            nbMountsForRecall++;
            castor::dlf::Param params[] =
              {castor::dlf::Param("TPVID", infoCandidate->vid)};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,ALLOWED_WITHOUT_POLICY , params);
          } else {
	      castor::dlf::Param paramsInput[] =
		{
		 castor::dlf::Param("TPVID", infoCandidate->vid),
		 castor::dlf::Param("numSegments", infoCandidate->numSegments),
		 castor::dlf::Param("totalBytes", infoCandidate->totalBytes),
		 castor::dlf::Param("ageOfOldestSegment", infoCandidate->ageOfOldestSegment),
		 castor::dlf::Param("priority", infoCandidate->priority),
		 castor::dlf::Param("nbMountsForRecall", nbMountsForRecall)  
		};
	
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, POLICY_INPUT, paramsInput);
	      
              // call the Python function for the RecallPolicy  
              bool priorityChosen=applyRecallPolicy(*infoCandidate,nbMountsForRecall);

              if ( priorityChosen ) {  
                eligibleTapeIds.push_back(infoCandidate->tapeId); // tape to resurrect
                nbMountsForRecall++;
                castor::dlf::Param params[] =
                  {
                   castor::dlf::Param("TPVID", infoCandidate->vid),
                   castor::dlf::Param("priority", infoCandidate->priority)
                  };
                castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, ALLOWED_BY_POLICY, params);
               } else {
               	 tagForRecallAllowed=false; //we do not have to update VDQM      
                 castor::dlf::Param params[] =
                   {castor::dlf::Param("TPVID", infoCandidate->vid)};
                   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, NOT_ALLOWED, params);
               }
          }
        } catch (castor::exception::Exception e) { // python exception
            castor::dlf::Param params[] =
              {
               castor::dlf::Param("code", sstrerror(e.code())),
               castor::dlf::Param("message", e.getMessage().str())
             };
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, PYTHON_ERROR , params);
    
            // we allow the recall
            eligibleTapeIds.push_back(infoCandidate->tapeId);
            nbMountsForRecall++;

            castor::dlf::Param params2[] =
              {castor::dlf::Param("TPVID", infoCandidate->vid)};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, ALLOWED_WITHOUT_POLICY, params2);
        }

      } else { // tape in status PENDING or WAITDRIVE
         ; // the tape already in VDQM 
           // we can only change priority in the queue here 
      }

      // if the recall is not allowed do not update VDQM with the priority
      if ( tagForRecallAllowed ) {
        gettimeofday(&tvStart, NULL);
	    
        vdqm_SendVolPriority((char*)infoCandidate->vid.c_str(),0,(int)infoCandidate->priority,0);
	    
        gettimeofday(&tvEnd, NULL);
        procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
        castor::dlf::Param paramsVdqm[] =
	  {
	   castor::dlf::Param("TPVID",infoCandidate->vid),
	   castor::dlf::Param("ProcessingTime", procTime * 0.000001),
	   castor::dlf::Param("Priority", infoCandidate->priority)
	  };
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, PRIORITY_SENT, paramsVdqm);
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
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, RESURRECT_TAPES, paramsDb2);
	     

    }

  } catch(castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("code", sstrerror(e.code())),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);
    
  } catch (...) {
    castor::dlf::Param params2[] =
      {castor::dlf::Param("message", "general exception caught")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params2);
    
  }
  
 
  eligibleTapeIds.clear();
  infoCandidateTape.clear();
}   

//-----------------------------------------------------------------
//  applyRecallPolicy
//-----------------------------------------------------------------

bool castor::tape::rechandler::RecHandlerThread::applyRecallPolicy(const RecallPolicyElement& elem, const int nbMountsForRecall)
  throw (castor::exception::Exception ){

  //apply the policy using a scoped lock as protection
  castor::tape::python::ScopedPythonLock scopedPythonLock;

  // if we don't have any function available we always retry to recall the tapecopy

  if (m_pyFunction == NULL)
    return true;

  // Create the input tuple for retry migration policy Python-function
  //
  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)

  castor::tape::python::SmartPyObjectPtr inputObj(Py_BuildValue("(s,K,K,K,K,K)", 
  	  (elem.vid).c_str(),elem.numSegments,elem.totalBytes,elem.ageOfOldestSegment,elem.priority,nbMountsForRecall));

  
  // Call the retry_migration-policy Python-function
  castor::tape::python::SmartPyObjectPtr resultObj(PyObject_CallObject(m_pyFunction, inputObj.get()));

  // Throw an exception if the recall-policy Python-function call failed
  if(resultObj.get() == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to execute recall-policy Python-function";

    throw ex;
  }

  // Return the result of the Python function
  const bool resultBool = PyInt_AsLong(resultObj.get());
  return resultBool;


}



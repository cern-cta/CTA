/******************************************************************************
 *                     StreamThread.cpp
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
 * @(#)$RCSfile: StreamThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/rtcopy/mighunter/DlfCodes.hpp"
#include "castor/rtcopy/mighunter/StreamThread.hpp"


#include "castor/infoPolicy/StreamPolicyElement.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/rtcopy/mighunter/IMigHunterSvc.hpp"
#include <Cns_api.h>
#include <getconfent.h>
#include <u64subr.h>
#include <list>

  


  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::rtcopy::mighunter::StreamThread::StreamThread(std::list<std::string> svcClassArray, castor::infoPolicy::StreamPySvc* strPy ){
  m_listSvcClass=svcClassArray;
  m_strSvc=strPy;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::StreamThread::run(void* par)
{

  // create service 

  // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraMigHunterSvc", castor::SVC_ORAMIGHUNTERSVC); 
  castor::rtcopy::mighunter::IMigHunterSvc* oraSvc = dynamic_cast<castor::rtcopy::mighunter::IMigHunterSvc*>(dbSvc);


  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }
  
  for(std::list<std::string>::iterator svcClassName=m_listSvcClass.begin(); 
      svcClassName != m_listSvcClass.end();
      svcClassName++){

    std::list<castor::infoPolicy::StreamPolicyElement> infoCandidateStreams;
    std::list<castor::infoPolicy::StreamPolicyElement> eligibleStreams;
    std::list<castor::infoPolicy::StreamPolicyElement> streamsToRestore;
    std::list<castor::infoPolicy::StreamPolicyElement>::iterator infoCandidateStream;


    try { // to catch exceptions specific of a svcclass

	
	// retrieve information from the db to know which stream should be started and attach the eligible tapecopy
      
	timeval tvStart,tvEnd;
	gettimeofday(&tvStart, NULL);

	oraSvc->inputForStreamPolicy(*svcClassName,infoCandidateStreams);
	
	gettimeofday(&tvEnd, NULL);
	signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
  
	castor::dlf::Param paramsDb[] =
	  {
	    castor::dlf::Param("SvcClass", *svcClassName),
	    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };
      
	if  (infoCandidateStreams.empty()) {
	  // log error and continue with the next svc class

	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NO_STREAM, 2, paramsDb);
	 
	  continue;
        } else {
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, STREAMS_FOUND, 2, paramsDb);
	}

	// call the policy for the different stream
	u_signed64 nbDrives=0;
	u_signed64 runningStreams=0;
	infoCandidateStream=infoCandidateStreams.begin();

	if (infoCandidateStream != infoCandidateStreams.end()){

	  // the following information are always the same in all the candidates for the same svcclass
	  nbDrives=(*infoCandidateStream).maxNumStreams();
	  runningStreams=(*infoCandidateStream).runningStream();
	}
 
        // counters for logging 

	int policyYes=0;
	int policyNo=0;
        int withoutPolicy=0;
	
	gettimeofday(&tvStart, NULL);

	for (infoCandidateStream=infoCandidateStreams.begin();
	     infoCandidateStream != infoCandidateStreams.end();
	     infoCandidateStream++){
	  

	  (*infoCandidateStream).setRunningStream(runningStreams); // new potential value
	  
	  // if there are no candidates there is no point to call the policy 

	  if ((*infoCandidateStream).numBytes()==0) {
	     streamsToRestore.push_back(*infoCandidateStream);
	     continue;
	  }


	  castor::dlf::Param paramsInput[] =
	      { 
		castor::dlf::Param("SvcClass", *svcClassName),
		castor::dlf::Param("stream id", (*infoCandidateStream).streamId()),
		castor::dlf::Param("running streams", (*infoCandidateStream).runningStream()),
		castor::dlf::Param("bytes attached", (*infoCandidateStream).numBytes()),
		castor::dlf::Param("number of files", (*infoCandidateStream).numFiles()),
		castor::dlf::Param("max number of streams", (*infoCandidateStream).maxNumStreams()),
		castor::dlf::Param("age", (*infoCandidateStream).age())
	      };
	  
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, STREAM_INPUT, 7, paramsInput);

	  castor::dlf::Param paramsOutput[] =
	      { 
		castor::dlf::Param("SvcClass", *svcClassName),
		castor::dlf::Param("stream id", (*infoCandidateStream).streamId())
	      };


          // start to apply the policy
  
	  try {
	    if (m_strSvc == NULL ||((*infoCandidateStream).policyName()).empty()){
	      //no policy
	      
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, START_WITHOUT_POLICY, 2, paramsOutput);
	      eligibleStreams.push_back(*infoCandidateStream);
	      
	      runningStreams++;
	      withoutPolicy++;
	     
	    } else {
	      //apply the policy
	      if (m_strSvc->applyPolicy(&(*infoCandidateStream))) {
		// policy yes
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, ALLOWED_BY_POLICY, 2, paramsOutput);
		
		eligibleStreams.push_back(*infoCandidateStream);
		runningStreams++;
		policyYes++;

	      } else {
		// policy no
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NOT_ALLOWED, 2, paramsOutput);
		streamsToRestore.push_back(*infoCandidateStream);
		policyNo++;
	      }
	    }


	  }catch (castor::exception::Exception e){
	    castor::dlf::Param params[] =
	      { castor::dlf::Param("policy","Stream Policy"),
		castor::dlf::Param("code", sstrerror(e.code())),
		castor::dlf::Param("stream",(*infoCandidateStream).streamId()),
		castor::dlf::Param("message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 4, params);
	    exit(-1);

	  }
	 
	}
	
	gettimeofday(&tvEnd, NULL);

	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 
        // log in the dlf with the summary
	castor::dlf::Param paramsPolicy[]={  
          castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("allowed",withoutPolicy),
	  castor::dlf::Param("allowedByPolicy",policyYes),
	  castor::dlf::Param("notAllowed",policyNo),
	  castor::dlf::Param("runningStreams",runningStreams),
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STREAM_POLICY_RESULT,6 , paramsPolicy);

	gettimeofday(&tvStart, NULL);
 
	// start streams

	oraSvc->startChosenStreams(eligibleStreams);
	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 
	castor::dlf::Param paramsStart[]={  
          castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STARTED_STREAMS,2 , paramsStart);

	// stop streams
	
	gettimeofday(&tvStart, NULL);

	oraSvc->stopChosenStreams(streamsToRestore);

	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 
	castor::dlf::Param paramsStop[]={  
          castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STOP_STREAMS,2 , paramsStop);


      } catch (castor::exception::Exception e){
	// exception due to problems specific to the service class
      } catch (...){}
	 
  } // loop for svcclass
}




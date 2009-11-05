/******************************************************************************
 *                      MigHunterThread.cpp
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
 * @(#)$RCSfile: MigHunterThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/
#include "castor/rtcopy/mighunter/DlfCodes.hpp"
#include "castor/rtcopy/mighunter/MigHunterThread.hpp"

#include "castor/infoPolicy/MigrationPolicyElement.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"


#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"
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
castor::rtcopy::mighunter::MigHunterThread::MigHunterThread(std::list<std::string> svcClassArray, u_signed64 minByte, bool doClone,castor::infoPolicy::MigrationPySvc* migrPy){
  m_listSvcClass=svcClassArray;
  m_byteVolume=minByte;
  m_doClone=doClone;
  m_migrSvc=migrPy;

}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterThread::run(void* par)
{

  
 
  // create service 

  // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraMigHunterSvc", castor::SVC_ORAMIGHUNTERSVC); 
  castor::rtcopy::mighunter::IMigHunterSvc* oraSvc = dynamic_cast<castor::rtcopy::mighunter::IMigHunterSvc*>(dbSvc);


  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
     return;
  }

  for (std::list<std::string>::iterator svcClassName=m_listSvcClass.begin(); 
       svcClassName != m_listSvcClass.end();
       svcClassName++){

    
    std::list<castor::infoPolicy::MigrationPolicyElement> infoCandidateTapeCopies;

    std::list<castor::infoPolicy::MigrationPolicyElement> candidatesToRestore;
    std::list<castor::infoPolicy::MigrationPolicyElement> invalidTapeCopies;
    
    std::list<castor::infoPolicy::MigrationPolicyElement>::iterator infoCandidate;
    
    std::map<u_signed64, std::list<castor::infoPolicy::MigrationPolicyElement> > eligibleCandidates;
    std::map<u_signed64, std::list<castor::infoPolicy::MigrationPolicyElement> >::iterator eligibleCandidate;
    
    try { // to catch exceptions specific of a svcclass


        // loop all the given  svc class
	u_signed64 initialSizeToTransfer=0;
      
	timeval tvStart,tvEnd;
	gettimeofday(&tvStart, NULL);

	// tapecopy id and extra information retrieved from the db for the python policy  

	
	oraSvc->inputForMigrationPolicy((*svcClassName), &initialSizeToTransfer,infoCandidateTapeCopies);

	gettimeofday(&tvEnd, NULL);
	signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
	
	castor::dlf::Param paramsDb[] =
	  {
	    castor::dlf::Param("SvcClass", *svcClassName),
	    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };
	

	if (infoCandidateTapeCopies.empty()){	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NO_TAPECOPIES, 2, paramsDb);
	  continue;
	} else {
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPECOPIES_FOUND, 2, paramsDb); 
	}
	

	// input for stream operation 

	char* p=NULL;
        u_signed64 initialSizeCeiling = 10UL*1024UL*1024UL*1024UL; // 10G
	if ( (p = getconfent("MigHunter","SIZECEILING",0)) != NULL ) {
	  initialSizeCeiling = strutou64(p);
	}
	
	gettimeofday(&tvStart, NULL);
          
	int ret=oraSvc->createOrUpdateStream((*svcClassName),initialSizeToTransfer,m_byteVolume,initialSizeCeiling,m_doClone,infoCandidateTapeCopies.size()); 
     // log error on DLF for the stream
	

	if (ret<0){
	 
	 // resurrect candidates
	  oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);

	  gettimeofday(&tvEnd, NULL);
	  procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
	  
	  castor::dlf::Param paramsNoStreams[]= {
	    castor::dlf::Param("SVCCLASS", *svcClassName),
	    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };

          if(ret==-1){
	 
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, NO_TAPEPOOLS, 2, paramsNoStreams);
	  }  
	  if (ret==-2){
	    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, NOT_ENOUGH, 2, paramsNoStreams);
	  }
	  if (ret==-3){
	    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, NO_DRIVES, 2, paramsNoStreams);
	  }
	  castor::exception::Internal ex;
	  throw ex;
	}
        // call the policy foreach tape copy
	
      	
	u_signed64 policyYes=0;
	u_signed64 policyNo=0;
        u_signed64 withoutPolicy=0;

	

	gettimeofday(&tvStart, NULL);

	for ( infoCandidate=infoCandidateTapeCopies.begin();
	      infoCandidate != infoCandidateTapeCopies.end();
	      infoCandidate++ ){

	  //add information from the Name Server 

	  try {
	  
	    getInfoFromNs(*svcClassName,*infoCandidate);
         
	  } catch (castor::exception::Exception e){

	    // note that we've already logged something inside getInfoFromNS
            invalidTapeCopies.push_back(*infoCandidate);
	    continue;
	  }  

	  struct Cns_fileid castorFileId;
	  memset(&castorFileId,'\0',sizeof(castorFileId));
	  strncpy(
		  castorFileId.server,
		  (*infoCandidate).nsHost().c_str(),
		  sizeof(castorFileId.server)-1
		  );
	  castorFileId.fileid = (*infoCandidate).fileId();


	  castor::dlf::Param paramsInput[] =
	      { 
		castor::dlf::Param("SvcClass", *svcClassName),
		castor::dlf::Param("tapecopy id", (*infoCandidate).tapeCopyId()),
		castor::dlf::Param("copy nb", (*infoCandidate).copyNb()),
		castor::dlf::Param("castorfilename", (*infoCandidate).castorFileName()),
		castor::dlf::Param("tapepoolname", (*infoCandidate).tapePoolName()),
		castor::dlf::Param("file mode", (*infoCandidate).fileMode()),
		castor::dlf::Param("nlink", (*infoCandidate).nlink()),
		castor::dlf::Param("uid", (*infoCandidate).uid()),
		castor::dlf::Param("gid", (*infoCandidate).gid()),
		castor::dlf::Param("fileSize", (*infoCandidate).fileSize()),
		castor::dlf::Param("aTime", (*infoCandidate).aTime()),
		castor::dlf::Param("mTime", (*infoCandidate).mTime()),
		castor::dlf::Param("cTime", (*infoCandidate).cTime()),
		castor::dlf::Param("fileClass", (*infoCandidate).fileClass()),
		castor::dlf::Param("status", (*infoCandidate).status()),
	      };
	  
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, POLICY_INPUT, 15, paramsInput,&castorFileId);

	  castor::dlf::Param paramsOutput[] =
	      { 
		castor::dlf::Param("SvcClass", *svcClassName),
		castor::dlf::Param("tapecopy id", (*infoCandidate).tapeCopyId())
	      };


	  // policy called for each tape copy for each tape pool
	  try {
  
	    if ( m_migrSvc == NULL || ((*infoCandidate).policyName()).empty() ) {
	      // no policy, I attached it
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, START_WITHOUT_POLICY, 2, paramsOutput, &castorFileId);
	       eligibleCandidates[(*infoCandidate).tapePoolId()].push_back(*infoCandidate);
	       withoutPolicy++;
	    } else {
	      if (m_migrSvc->applyPolicy(&(*infoCandidate))){
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, ALLOWED_BY_POLICY, 2, paramsOutput, &castorFileId);
		eligibleCandidates[(*infoCandidate).tapePoolId()].push_back(*infoCandidate);
		policyYes++;
	      } else {
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NOT_ALLOWED, 2, paramsOutput,&castorFileId);
		policyNo++;

	      }
	    }
	    	  
	  } catch (castor::exception::Exception e){
	    struct Cns_fileid castorFileId;
	    memset(&castorFileId,'\0',sizeof(castorFileId));
	    strncpy(
		    castorFileId.server,
		    (*infoCandidate).nsHost().c_str(),
		    sizeof(castorFileId.server)-1
		    );
	    castorFileId.fileid = (*infoCandidate).fileId();
	    
	    castor::dlf::Param params[] =
	      { castor::dlf::Param("SvcClass", (*svcClassName)),
		castor::dlf::Param("policy", "Migration Policy"),
		castor::dlf::Param("code", sstrerror(e.code())),
		castor::dlf::Param("message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 4, params,&castorFileId);
	    exit(-1);

	  }

	 
	}


	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 
	

        // log in the dlf with the summary

	castor::dlf::Param paramsPolicy[]={  
          castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("allowed",withoutPolicy),
	  castor::dlf::Param("allowedByPolicy",policyYes),
	  castor::dlf::Param("notAllowed",policyNo),
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	  };

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, POLICY_RESULT, 5, paramsPolicy);


	for (eligibleCandidate=eligibleCandidates.begin(); 
	       eligibleCandidate != eligibleCandidates.end();
	       eligibleCandidate++){

	  try {
	    //attach the eligible tape copies
	    gettimeofday(&tvStart, NULL);

	    oraSvc->attachTapeCopiesToStreams((*eligibleCandidate).second);

	    gettimeofday(&tvEnd, NULL);
	    procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 

	    castor::dlf::Param paramsDbUpdate[]={  
	      castor::dlf::Param("SvcClass",(*svcClassName)),
	      castor::dlf::Param("tapepool", (*eligibleCandidate).first),
	      castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	    };

	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, ATTACHED_TAPECOPIES, 3, paramsDbUpdate);

	  }catch(castor::exception::Exception e){
	    castor::dlf::Param params[] =
	      {
		castor::dlf::Param("SvcClass",(*svcClassName)),
		castor::dlf::Param("code", sstrerror(e.code())),
		castor::dlf::Param("message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DB_ERROR, 3, params); 
	    oraSvc->resurrectTapeCopies((*eligibleCandidate).second);
	  }
	  
	 
	  
	}
	
	// resurrect tape copies not eligible
	
	
	gettimeofday(&tvStart, NULL);

	oraSvc->resurrectTapeCopies(candidatesToRestore);

	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 
	castor::dlf::Param paramsResurrect[]={  
	  castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("nb tapecopies",candidatesToRestore.size()),
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, RESURRECT_TAPECOPIES, 3, paramsResurrect);
      
	// delete tapecopies which refers to files which are not in the nameserver


	gettimeofday(&tvStart, NULL);
	oraSvc->invalidateTapeCopies(invalidTapeCopies);

	gettimeofday(&tvEnd, NULL);
	procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
 
	castor::dlf::Param paramsInvalidate[]={  
	  castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("nb tapecopies",invalidTapeCopies.size()),
	  castor::dlf::Param("ProcessingTime", procTime * 0.000001)
	};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, INVALIDATE_TAPECOPIES, 3, paramsInvalidate);


      } catch (castor::exception::Exception e){
	// exception due to problems specific to the service class
      } catch (...){}

      // added in the svclass loop
   
  } // loop for svcclass
 
}


void castor::rtcopy::mighunter::MigHunterThread::getInfoFromNs
(std::string svcClassName, castor::infoPolicy::MigrationPolicyElement& elem) throw (castor::exception::Exception) {
  struct Cns_filestat statbuf;
  struct Cns_fileid cnsFile;
  memset(&cnsFile,'\0',sizeof(cnsFile));
  cnsFile.fileid=elem.fileId();
  char castorFileName[CA_MAXPATHLEN+1];
  castorFileName[0] = '\0';
  strncpy(cnsFile.server,elem.nsHost().c_str(),sizeof(cnsFile.server)-1);
  
  int rc = Cns_statx(castorFileName,&cnsFile,&statbuf);
  if (rc == -1){
    castor::dlf::Param paramsError[]={
      castor::dlf::Param("SvcClass", svcClassName),
      castor::dlf::Param("Function", "Cns_statx"),
      castor::dlf::Param("Error", sstrerror(serrno))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR, NS_ERROR, 3, paramsError, &cnsFile);
    castor::exception::Exception e(serrno);
    throw e;
  }  
    
  elem.setFileId(statbuf.fileid);
  elem.setFileMode((u_signed64)statbuf.filemode);
  elem.setNlink(statbuf.nlink);
  elem.setUid((u_signed64)statbuf.uid);
  elem.setGid((u_signed64)statbuf.gid);
  elem.setFileSize(statbuf.filesize);
  elem.setATime((u_signed64)statbuf.atime);
  elem.setMTime((u_signed64)statbuf.mtime);
  elem.setCTime((u_signed64)statbuf.ctime);
  elem.setFileClass((int)statbuf.fileclass);
  elem.setStatus((unsigned char)statbuf.status); 
  
}


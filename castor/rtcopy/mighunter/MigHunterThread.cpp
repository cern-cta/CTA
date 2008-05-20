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

#include "castor/rtcopy/mighunter/MigHunterThread.hpp"

//#include "castor/infoPolicy/PySvc.hpp"
#include "castor/infoPolicy/DbInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/DbInfoStreamPolicy.hpp"
#include "castor/infoPolicy/DbInfoPolicy.hpp"
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

#include <Cns_api.h>
#include <getconfent.h>
#include <u64subr.h>

  


  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::rtcopy::mighunter::MigHunterThread::MigHunterThread(castor::infoPolicy::IPolicySvc* svc, std::vector<std::string> svcClassArray, u_signed64 minByte, bool doClone,castor::infoPolicy::MigrationPySvc* migrPy,castor::infoPolicy::StreamPySvc* strPy ){
  m_policySvc=svc; 
  m_listSvcClass=svcClassArray;
  m_byteVolume=minByte;
  m_doClone=doClone;
  m_migrSvc=migrPy;
  m_strSvc=strPy;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterThread::run(void* par)
{
  std::vector<castor::infoPolicy::PolicyObj*> infoCandidateTapeCopies;
  std::vector<castor::infoPolicy::PolicyObj*> infoCandidateStreams;
  std::vector<castor::infoPolicy::PolicyObj*> eligibleCandidates;
  std::vector<castor::infoPolicy::PolicyObj*> candidatesToRestore;
  std::vector<castor::infoPolicy::PolicyObj*> invalidTapeCopies;
  std::vector<castor::infoPolicy::PolicyObj*> eligibleStreams;
  std::vector<castor::infoPolicy::PolicyObj*> streamsToRestore;
  std::vector<castor::infoPolicy::PolicyObj*>::iterator infoCandidate;
  std::vector<castor::infoPolicy::PolicyObj*>::iterator infoCandidateStream;

  castor::infoPolicy::DbInfoMigrationPolicy* realInfo=NULL;
  castor::infoPolicy::DbInfoStreamPolicy* streamInfo=NULL;
  castor::infoPolicy::CnsInfoMigrationPolicy* cnsInfo=NULL;

  std::vector<std::string>::iterator svcClassName=m_listSvcClass.begin();

  while(svcClassName != m_listSvcClass.end()){

      try { // to catch exceptions specific of a svcclass

	castor::dlf::Param params0[] = {castor::dlf::Param("SvcClass", *svcClassName)};
        // loop all the given  svc class
	u_signed64 initialSizeToTransfer=0;
      
	// tapecopy id and extra information retrieved from the db for the python policy       
   
	infoCandidateTapeCopies=m_policySvc->inputForMigrationPolicy((*svcClassName),&initialSizeToTransfer);

	if (infoCandidateTapeCopies.empty()){	
	   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 3, 1, params0);
	}

	// input for stream operation 

	char* p=NULL;
        u_signed64 initialSizeCeiling = 10UL*1024UL*1024UL*1024UL; // 10G
	if ( (p = getconfent("MigHunter","SIZECEILING",0)) != NULL ) {
	  initialSizeCeiling = strutou64(p);
	}
      
          
	int ret=m_policySvc->createOrUpdateStream((*svcClassName),initialSizeToTransfer,m_byteVolume,initialSizeCeiling,m_doClone,infoCandidateTapeCopies); 
     // log error on DLF for the stream

	if (ret<0){
	 // resurrect candidates
	  m_policySvc->resurrectTapeCopies(infoCandidateTapeCopies);
          if(ret==-1){
	    castor::dlf::Param params1[]= {castor::dlf::Param("message", "No tapepools Candidate")};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 1, params1);
	  }  
           if (ret==-2){
	     castor::dlf::Param params1[]={castor::dlf::Param("message", "Not enough data")};
	     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, 4, 1, params1);
	   }
	   if (ret==-3){
	     castor::dlf::Param params1[]={castor::dlf::Param("message", "nbDrives zero")};
	     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 1, params1);
	   }
	   castor::exception::Internal ex;
	   ex.getMessage()
	    << "Error in create or update stream.";
	}
         
	// call the policy foreach tape copy

	infoCandidate=infoCandidateTapeCopies.begin();
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 5, 1, params0);
      	
	u_signed64 policyYes=0;
	u_signed64 policyNo=0;
        u_signed64 withoutPolicy=0;

	while ( infoCandidate != infoCandidateTapeCopies.end() ){

	  //add information from the Name Server 

	  realInfo=dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*> ((*infoCandidate)->dbInfoPolicy()[0]);

	  if (realInfo == NULL){
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 1, params0);
	    infoCandidate++;
	    continue;
	  }

	  cnsInfo = getInfoFromNs(realInfo->nsHost(),realInfo->fileId());
         
	  if (cnsInfo==NULL) {
	    //not in the nameServer
            invalidTapeCopies.push_back(*infoCandidate);
            infoCandidate++;
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6, 1, params0);
	    continue;
	  }  

	  (*infoCandidate)->addCnsInfoPolicy(cnsInfo);

	  // policy called for each tape copy for each tape pool
	  try {
  
	    if ( m_migrSvc == NULL || ((*infoCandidate)->policyName()).empty() ) {
	      // no policy, I attached it
	       eligibleCandidates.push_back(*infoCandidate);
	       withoutPolicy++;
	    } else {
	      if (m_migrSvc->applyPolicy(*infoCandidate)){
		eligibleCandidates.push_back(*infoCandidate);
		policyYes++;
	      } else {
		candidatesToRestore.push_back(*infoCandidate);
		policyNo++;

	      }
	    }
	    	  
	  } catch (castor::exception::Exception e){
	    castor::dlf::Param params[] =
	      { castor::dlf::Param("SvcClass", (*svcClassName)),
		castor::dlf::Param("policy", "Migration Policy"),
		castor::dlf::Param("fileId", realInfo->fileId()),
		castor::dlf::Param("code", sstrerror(e.code())),
		castor::dlf::Param("message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 5, params);
	    // restore the candidate
	    candidatesToRestore.push_back(*infoCandidate);
	  }
	  infoCandidate++;
	}

	// Log in DLF the summary
	castor::dlf::Param params2[]={  
          castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("allowed",withoutPolicy),
	  castor::dlf::Param("allowedByPolicy",policyYes),
	  castor::dlf::Param("notAllowed",policyNo)
	  };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 7,4 , params2);


	try {
	//attach the eligible tape copies
	  m_policySvc->attachTapeCopiesToStreams(eligibleCandidates);
	}catch(castor::exception::Exception e){
	  castor::dlf::Param params[] =
	    {
	     castor::dlf::Param("SvcClass",(*svcClassName)),
	     castor::dlf::Param("code", sstrerror(e.code())),
	     castor::dlf::Param("message", e.getMessage().str())};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 8, 3, params);

	  m_policySvc->resurrectTapeCopies(eligibleCandidates);
	  m_policySvc->resurrectTapeCopies(candidatesToRestore);
	  castor::exception::Internal ex;
	  ex.getMessage()
	    << "Error in attaching tapecopies";

	}

	// resurrect tape copies not eligible

	m_policySvc->resurrectTapeCopies(candidatesToRestore);
      
	// delete tapecopies which refers to files which are not in the nameserver

	m_policySvc->invalidateTapeCopies(invalidTapeCopies);


	// retrieve information from the db to know which stream should be started and attach the eligible tapecopy
      
       
	infoCandidateStreams=m_policySvc->inputForStreamPolicy((*svcClassName));
      
	if  (infoCandidateStreams.empty()) {
	  // log error and continue with the next svc class
	  castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) )};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 9, 1, params4);
	  castor::exception::Internal ex;
	  ex.getMessage()
	    << "No streams available";
        }

	// call the policy for the different stream
	u_signed64 nbDrives=0;
	u_signed64 runningStreams=0;
	infoCandidateStream=infoCandidateStreams.begin();

	if (infoCandidateStream != infoCandidateStreams.end()){
	  streamInfo=dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*> ((*infoCandidateStream)->dbInfoPolicy()[0]);
	  // the following information are always the same in all the candidates for the same svcclass
	  
	  nbDrives=streamInfo->maxNumStreams();
	  runningStreams=streamInfo->runningStream();
	}
 
        // counters for logging 
	policyYes=0;
	policyNo=0;
        withoutPolicy=0;
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 10, 1, params0);

	while (infoCandidateStream != infoCandidateStreams.end()){
	  streamInfo=NULL;
	  if (!((*infoCandidateStream)->dbInfoPolicy()).empty())
	    streamInfo=dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*> ((*infoCandidateStream)->dbInfoPolicy()[0]);

	  if (streamInfo == NULL){
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 14, 1, params0);
	    continue;
	  }

	  streamInfo->setRunningStream(runningStreams); // new potential value
          // start to apply the policy
  
	  try {
	    if (m_strSvc == NULL ||((*infoCandidateStream)->policyName()).empty()){
	      //no policy
	      eligibleStreams.push_back(*infoCandidateStream);
	      runningStreams++;
	      withoutPolicy++;
	     
	    } else {
	      //apply the policy
	      if (m_strSvc->applyPolicy(*infoCandidateStream)) {
		// policy yes
		 eligibleStreams.push_back(*infoCandidateStream);
		 runningStreams++;
		 policyYes++;

	      } else {
		// policy no
		 streamsToRestore.push_back(*infoCandidateStream);
		 policyNo++;
	      }
	    }

	  }catch (castor::exception::Exception e){
	    castor::dlf::Param params[] =
	      { castor::dlf::Param("policy","Stream Policy"),
		castor::dlf::Param("code", sstrerror(e.code())),
		castor::dlf::Param("stream",streamInfo->streamId()),
		castor::dlf::Param("message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 4, params);
	  }
	  infoCandidateStream++;
	}

        // log in the dlf with the summary
	castor::dlf::Param params[]={  
          castor::dlf::Param("SvcClass",(*svcClassName)),
	  castor::dlf::Param("allowed",withoutPolicy),
	  castor::dlf::Param("allowedByPolicy",policyYes),
	  castor::dlf::Param("notAllowed",policyNo),
	  castor::dlf::Param("runningStreams",runningStreams)
	  };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 11,5 , params);

	u_signed64 initialSizeForStream =0;
        runningStreams= runningStreams > nbDrives? nbDrives:runningStreams;
        if (runningStreams !=0)  
	  initialSizeForStream = (u_signed64) initialSizeToTransfer/runningStreams;
	initialSizeForStream= (initialSizeCeiling > 0 && initialSizeForStream>initialSizeCeiling)?initialSizeCeiling:initialSizeForStream;


	m_policySvc->startChosenStreams(eligibleStreams,initialSizeForStream);

	m_policySvc->stopChosenStreams(streamsToRestore);

      } catch (castor::exception::Exception e){
	// exception due to problems specific to the service class
      } catch (...){}
	 
      // CLEANUP 
     
      // clean up tapecopies

      infoCandidate=infoCandidateTapeCopies.begin();
      while ( infoCandidate !=infoCandidateTapeCopies.end() ){
         if (*infoCandidate){
	   realInfo=NULL;
	   if (!(*infoCandidate)->dbInfoPolicy().empty())
	     realInfo=dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*> ((*infoCandidate)->dbInfoPolicy()[0]);

	    cnsInfo=NULL;
	    if (!(*infoCandidate)->cnsInfoPolicy().empty())
	     cnsInfo =dynamic_cast<castor::infoPolicy::CnsInfoMigrationPolicy*> ((*infoCandidate)->cnsInfoPolicy()[0]);

	    delete *infoCandidate;
	   *infoCandidate=NULL;
	     
	   if (realInfo)delete realInfo;
	   realInfo=NULL;
	   if (cnsInfo) delete  cnsInfo;
	   cnsInfo=NULL;

	   infoCandidate++;
	}
      }

      // clean up stream
 
      infoCandidateStream=infoCandidateStreams.begin();
      while (infoCandidateStream != infoCandidateStreams.end()){
        if (*infoCandidateStream){
	   streamInfo=NULL;
	   if (!(*infoCandidateStream)->dbInfoPolicy().empty())
	     streamInfo=dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*> ((*infoCandidateStream)->dbInfoPolicy()[0]);
	   delete *infoCandidateStream;
	   *infoCandidateStream=NULL;
	   if (streamInfo)delete streamInfo;
	   streamInfo=NULL;
	}
	   infoCandidateStream++;
       } 
       
      // cleanup vectors

      infoCandidateTapeCopies.clear();
      eligibleCandidates.clear();
      candidatesToRestore.clear();
      invalidTapeCopies.clear();
      infoCandidateStreams.clear();
      eligibleStreams.clear();
      streamsToRestore.clear();
   
      // new SvcClass

      svcClassName++;

  } // loop for svcclass
}


castor::infoPolicy::CnsInfoMigrationPolicy* castor::rtcopy::mighunter::MigHunterThread::getInfoFromNs(std::string nsHost,u_signed64 fileId){
  struct Cns_filestat statbuf;
  struct Cns_fileid cnsFile;
  memset(&cnsFile,'\0',sizeof(cnsFile));
  cnsFile.fileid=fileId;
  char castorFileName[CA_MAXPATHLEN+1];
  castorFileName[0] = '\0';
  strncpy(cnsFile.server,nsHost.c_str(),sizeof(cnsFile.server)-1);
  
  int rc = Cns_statx(castorFileName,&cnsFile,&statbuf);
  if (rc == -1){
    castor::dlf::Param params2[]={
      castor::dlf::Param("Function", "Cns_statx"),
      castor::dlf::Param("Error", sstrerror(serrno))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6, 2, params2, &cnsFile);
    return NULL;
  }  

  castor::infoPolicy::CnsInfoMigrationPolicy* result=new castor::infoPolicy::CnsInfoMigrationPolicy();     
  result->setFileId(statbuf.fileid);
  result->setFileMode((u_signed64)statbuf.filemode);
  result->setNlink(statbuf.nlink);
  result->setUid((u_signed64)statbuf.uid);
  result->setGid((u_signed64)statbuf.gid);
  result->setFileSize(statbuf.filesize);
  result->setATime((u_signed64)statbuf.atime);
  result->setMTime((u_signed64)statbuf.mtime);
  result->setCTime((u_signed64)statbuf.ctime);
  result->setFileClass((int)statbuf.fileclass);
  result->setStatus((unsigned char)statbuf.status);
  
  return result;
}


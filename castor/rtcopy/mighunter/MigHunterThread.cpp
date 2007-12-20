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
 * @(#)$RCSfile: MigHunterThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/rtcopy/mighunter/MigHunterThread.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/SvcClass.hpp"

#include <Cns_api.h>
#include <getconfent.h>
#include <u64subr.h>

#include "castor/infoPolicy/PySvc.hpp"
#include "castor/infoPolicy/MigrationPySvc.hpp"
#include "castor/infoPolicy/StreamPySvc.hpp"
#include "castor/infoPolicy/DbInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/DbInfoStreamPolicy.hpp"
#include "castor/infoPolicy/CnsInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/DbInfoPolicy.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"


  
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
 std::vector<castor::infoPolicy::PolicyObj*> eligibleStreams;
 std::vector<castor::infoPolicy::PolicyObj*> streamsToRestore;
 std::vector<castor::infoPolicy::PolicyObj*>::iterator infoCandidate;
 std::vector<castor::infoPolicy::PolicyObj*>::iterator infoCandidateStream;

 castor::infoPolicy::DbInfoMigrationPolicy* realInfo=NULL;
 castor::infoPolicy::DbInfoStreamPolicy* streamInfo=NULL;
 castor::infoPolicy::CnsInfoMigrationPolicy* cnsInfo=NULL;

 try{
    std::vector<std::string>::iterator svcClassName=m_listSvcClass.begin();
    while(svcClassName != m_listSvcClass.end()){
    // loop all the given  svc class

      u_signed64 initialSizeToTransfer=0;
      
      // tapecopy id and extra information retrieved from the db for the python policy       
   
      infoCandidateTapeCopies=m_policySvc->inputForMigrationPolicy((*svcClassName),&initialSizeToTransfer);

      if (infoCandidateTapeCopies.empty()){
	
	castor::dlf::Param params0[] =
        {castor::dlf::Param("SvcClass", *svcClassName),
	 castor::dlf::Param("message", "No tape copy found to be attached")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 3, 2, params0);
	

      }
      // input for stream operation 

      char* p=NULL;
      u_signed64 initialSizeCeiling=0;
      if ( (p = getconfent("MigHunter","SIZECEILING",1)) != NULL ) {
	 initialSizeCeiling = strutou64(p);
      }
      
          
      int ret=m_policySvc->createOrUpdateStream((*svcClassName),initialSizeToTransfer,m_byteVolume,initialSizeCeiling,m_doClone,infoCandidateTapeCopies); // all the tapecopies to restore them in case of problems
     // log error on DLF for the stream

       if (ret<0){
	 // resurrect candidates
	  m_policySvc->resurrectTapeCopies(infoCandidateTapeCopies);
          if(ret==-1){
	    castor::dlf::Param params[]= {castor::dlf::Param("message", "No tapepools Candidate")};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 1, params);
	  }  
           if (ret==-2){
	     castor::dlf::Param params2[]={castor::dlf::Param("message", "Not enough data")};
	     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 1, params2);
	   }
	   if (ret==-3){
	     // Fatal error but  continue with this svcclass
	     castor::dlf::Param params3[]={castor::dlf::Param("message", "Not data found error for create or update Stream")};
	     castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 1, params3);
	   }
       }
         
      // call the policy foreach tape copy

      infoCandidate=infoCandidateTapeCopies.begin();

      while ( infoCandidate != infoCandidateTapeCopies.end() ){

	//add information from the Name Server 

        realInfo=dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*> ((*infoCandidate)->dbInfoPolicy()[0]);

	cnsInfo = getInfoFromNs(realInfo->nsHost(),realInfo->fileId());

	// if CnsInfo has fileid equals '0' then
        // it is just there to keep the index place  

	(*infoCandidate)->addCnsInfoPolicy(cnsInfo);

       // policy called for each tape copy for each tape pool
 	try {
  
	  if ( m_migrSvc == NULL || ((*infoCandidate)->policyName()).empty() ||  m_migrSvc->applyPolicy(*infoCandidate)){
	    eligibleCandidates.push_back(*infoCandidate); // tapecopy has to be attached to the stream 
	    
	    if (m_migrSvc != NULL && !((*infoCandidate)->policyName()).empty() ) {
	      castor::dlf::Param params2[]={castor::dlf::Param("fileId", realInfo->fileId()),					      castor::dlf::Param("policy", "Migration Policy used")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 5, 2, params2);
	    } else {
	      castor::dlf::Param params2[]={castor::dlf::Param("fileId", realInfo->fileId()),					  castor::dlf::Param("policy", "No Policy used")};
	      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, 5, 2, params2);
	    }
	
	  } else { 
	    candidatesToRestore.push_back(*infoCandidate);
	     castor::dlf::Param params2[]={
	       castor::dlf::Param("message", realInfo->fileId()),
	       castor::dlf::Param("policy", "Migration Policy used")};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 6, 1, params2);
	  }
	  
	} catch (castor::exception::Exception e){
         castor::dlf::Param params[] =
        {
	 castor::dlf::Param("policy", "Migration Policy"),
	 castor::dlf::Param("fileId", realInfo->fileId()),
	 castor::dlf::Param("code", sstrerror(e.code())),
         castor::dlf::Param("message", e.getMessage().str())};
         castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 4, params);
	 // restore the candidate
	 candidatesToRestore.push_back(*infoCandidate);
	}


	infoCandidate++;
      }

      //attach the eligible tape copies

       m_policySvc->attachTapeCopiesToStreams(eligibleCandidates);


       // resurrect Tape copies not eligible

       m_policySvc->resurrectTapeCopies(candidatesToRestore);


      // retrieve information from the db to know which stream should be started and attach the eligible tapecopy
      
       
      infoCandidateStreams=m_policySvc->inputForStreamPolicy((*svcClassName));
      
      if  (infoCandidateStreams.empty() ||  infoCandidateStreams[0] == NULL) {
	// log error and continue with the next svc class
	castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
        castor::dlf::Param("message", "Not data found error for input for  stream policy")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 7, 2, params4);
 
      } else {

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

	while (infoCandidateStream != infoCandidateStreams.end()){
	  streamInfo=NULL;
	  if (!((*infoCandidateStream)->dbInfoPolicy()).empty())
	    streamInfo=dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*> ((*infoCandidateStream)->dbInfoPolicy()[0]);

	  if (streamInfo == NULL){
	    castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
					    castor::dlf::Param("message", "retrieving stream information not possible")};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 11, 2, params4);
	    continue;
	  }

	  streamInfo->setRunningStream(runningStreams); // initial value
	  try {
	    if (m_strSvc == NULL ||((*infoCandidateStream)->policyName()).empty() || m_strSvc->applyPolicy(*infoCandidateStream)){
	  // new one potentially  running
	      if (streamInfo->status() != castor::stager::STREAM_RUNNING ){ 
		runningStreams++;
		eligibleStreams.push_back(*infoCandidateStream);
		if (m_strSvc != NULL && !((*infoCandidateStream)->policyName()).empty() ) {
		  castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
						castor::dlf::Param("policy","Stream policy" ),
					        castor::dlf::Param("message", "stream allowed to start")};
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 8, 3, params4);
		} else {
		  castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
						castor::dlf::Param("policy","no policy used" ),
					        castor::dlf::Param("message", "stream allowed to start")};
		  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 8, 3, params4);
		
		}
	      }

	      if (m_strSvc != NULL && !((*infoCandidateStream)->policyName()).empty()) {
		castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
					      castor::dlf::Param("policy","Stream policy" ),
					      castor::dlf::Param("message", "running stream not stopped")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 8, 2, params4);
	      } else {

		castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
					      castor::dlf::Param("policy","no policy used" ),
					      castor::dlf::Param("message", "running stream not stopped")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 8, 2, params4);

	      }

	    } else {

	      // to be stopped and it was running or not allowed to start
	      streamsToRestore.push_back(*infoCandidateStream);
	      if (streamInfo->status() == castor::stager::STREAM_RUNNING ) {
		runningStreams--;
		// it is enough not to be in the list to be stopped

		castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
					       castor::dlf::Param("message", "running stream to be stopped")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 9, 2, params4);
	      }else {
		castor::dlf::Param params4 []= {castor::dlf::Param("SvcClass",(*svcClassName) ),
					       castor::dlf::Param("message", "stream not  allowed to start")};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 9, 2, params4);
	      }
	    }
	  }catch (castor::exception::Exception e){
	    castor::dlf::Param params[] =
	      { castor::dlf::Param("policy","Stream Policy"),
		castor::dlf::Param("code", sstrerror(e.code())),
		castor::dlf::Param("stream",streamInfo->streamId()),
		castor::dlf::Param("message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 4, params);
	  }

	  infoCandidateStream++;
	}

	u_signed64 initialSizeForStream =(u_signed64) initialSizeToTransfer/nbDrives;
	initialSizeForStream= (initialSizeCeiling > 0 && initialSizeForStream>initialSizeCeiling)?initialSizeCeiling:initialSizeForStream;
     
	m_policySvc->startChosenStreams(eligibleStreams,initialSizeForStream);
	m_policySvc->stopChosenStreams(streamsToRestore);

      } 
	 // CLEANUP

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

      // delete stream policy object
 
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
      infoCandidateStreams.clear();
      eligibleStreams.clear();
      streamsToRestore.clear();
   
      // new SvcClass

      svcClassName++;
    } // loop for svcclass
  }
   catch(castor::exception::Exception e){
       castor::dlf::Param params[] =
        {castor::dlf::Param("code", sstrerror(e.code())),
         castor::dlf::Param("message", e.getMessage().str())};
         castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 11, 2, params);
	 // CLEANUP

      // delete migration policy object
 
      infoCandidate=infoCandidateTapeCopies.begin();
      while ( infoCandidate != infoCandidateTapeCopies.end() ){
         if (*infoCandidate){
	   realInfo=NULL;
	   if (!(*infoCandidate)->dbInfoPolicy().empty())
	     realInfo=dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*> ((*infoCandidate)->dbInfoPolicy()[0]);
	   cnsInfo =NULL;
           if (!(*infoCandidate)->cnsInfoPolicy().empty())
	     cnsInfo = dynamic_cast<castor::infoPolicy::CnsInfoMigrationPolicy*> ((*infoCandidate)->cnsInfoPolicy()[0]);
	  
	   delete *infoCandidate;
	   *infoCandidate=NULL;
	   infoCandidate++;

	   if (realInfo)delete realInfo;
	   realInfo=NULL;
	   if (cnsInfo) delete  cnsInfo;
	   cnsInfo=NULL;

	   infoCandidate++;
	}
      }
      


      // delete stream policy object
 
      infoCandidateStream=infoCandidateStreams.begin();
      while (infoCandidateStream != infoCandidateStreams.end()){
        if (*infoCandidateStream){
	  streamInfo=NULL; 
	  if (! (*infoCandidateStream)->dbInfoPolicy().empty()) 
	     streamInfo=dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*> ((*infoCandidateStream)->dbInfoPolicy()[0]);
	  delete *infoCandidateStream;
	  *infoCandidateStream=NULL;

	  if (streamInfo)delete streamInfo;
	  streamInfo=NULL;

	}
	infoCandidateStream++;
       } 
       
      // clear vectors
      infoCandidateTapeCopies.clear();
      eligibleCandidates.clear();
      candidatesToRestore.clear();
      infoCandidateStreams.clear();
      eligibleStreams.clear();
      streamsToRestore.clear();

  }
    catch (...){
     castor::dlf::Param params2[] =
        {castor::dlf::Param("message", "general exception caught")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 11, 1, params2);
	 
      // CLEANUP


      // delete migration policy object
 
      infoCandidate=infoCandidateTapeCopies.begin();
      while ( infoCandidate !=eligibleCandidates.end() ){
         if (*infoCandidate){
	   realInfo=NULL;
	   if(! (*infoCandidate)->dbInfoPolicy().empty())
	     realInfo=dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*> ((*infoCandidate)->dbInfoPolicy()[0]);

	   cnsInfo=NULL;
	   if (! (*infoCandidate)->cnsInfoPolicy().empty())
	     cnsInfo =dynamic_cast<castor::infoPolicy::CnsInfoMigrationPolicy*> ((*infoCandidate)->cnsInfoPolicy()[0]);
	   
	   delete *infoCandidate;
	   *infoCandidate=NULL;
	   
	   if (realInfo)delete realInfo;
	   realInfo=NULL;
	   if (cnsInfo) delete cnsInfo;
	   cnsInfo=NULL;
	   
	   infoCandidate++;
	}
      }
      

      // delete stream policy object
 
      infoCandidateStream=infoCandidateStreams.begin();
      while (infoCandidateStream != infoCandidateStreams.end()){
        if (*infoCandidateStream){
	  streamInfo=NULL;
	  if (! (*infoCandidateStream)->dbInfoPolicy().empty())
	    streamInfo=dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*> ((*infoCandidateStream)->dbInfoPolicy()[0]);
	  if (streamInfo)delete streamInfo;
	  streamInfo=NULL;
	  delete *infoCandidateStream;
	  *infoCandidateStream=NULL;
	}
	   infoCandidateStream++;
       } 

      // clear vectors

      infoCandidateTapeCopies.clear();
      eligibleCandidates.clear();
      candidatesToRestore.clear();
      infoCandidateStreams.clear();
      eligibleStreams.clear();
      streamsToRestore.clear();

    }
}

castor::infoPolicy::CnsInfoMigrationPolicy* castor::rtcopy::mighunter::MigHunterThread::getInfoFromNs(std::string nsHost,u_signed64 fileId){
     castor::infoPolicy::CnsInfoMigrationPolicy* result=new castor::infoPolicy::CnsInfoMigrationPolicy();
      struct Cns_filestat statbuf;
      struct Cns_fileid cnsFile;
      memset(&cnsFile,'\0',sizeof(cnsFile));
      cnsFile.fileid=fileId;
      char castorFileName[CA_MAXPATHLEN+1];
      castorFileName[0] = '\0';
      strncpy(cnsFile.server,nsHost.c_str(),sizeof(cnsFile.server)-1);
      int rc = Cns_statx(castorFileName,&cnsFile,&statbuf);
      if (rc == -1){
           castor::dlf::Param params2[] =
        {castor::dlf::Param("fileid", fileId)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 10, 1, params2);
	result->setFileId(0);
        return result;
      }
        
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


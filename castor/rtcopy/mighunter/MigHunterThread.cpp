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

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/infoPolicy/MigrationPolicyElement.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include "castor/rtcopy/mighunter/IMigHunterSvc.hpp"
#include "castor/rtcopy/mighunter/MigHunterDlfMessageConstants.hpp"
#include "castor/rtcopy/mighunter/MigHunterThread.hpp"
#include "castor/rtcopy/mighunter/ScopedPythonLock.hpp"
#include "castor/rtcopy/mighunter/SmartPyObjectPtr.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"
#include "h/Cns_api.h"
#include "h/getconfent.h"

#include <list>
#include <stdlib.h>
#include <sys/types.h>
#include <u64subr.h>
#include <unistd.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::rtcopy::mighunter::MigHunterThread::MigHunterThread(
  const std::list<std::string> &svcClassList,
  const uint64_t               migrationDataThreshold,
  const bool                   doClone,
  PyObject *const              migrationPolicyDict) throw() :

  m_listSvcClass(svcClassList),
  m_migrationDataThreshold(migrationDataThreshold),
  m_doClone(doClone),
  m_migrationPolicyDict(migrationPolicyDict) {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterThread::run(void* arg) {

  try {

    exceptionThrowingRun(arg);

  } catch(castor::exception::Exception &ex) {

    // Log the exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);
  }
}


//------------------------------------------------------------------------------
// exceptionThrowingRun
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterThread::exceptionThrowingRun(
  void *arg) throw(castor::exception::Exception) {

  // Get a handle on the service to access the database
  const char *const oraSvcName = "OraMigHunterSvc";
  castor::IService* dbSvc =
    castor::BaseObject::services()->service(oraSvcName,
    castor::SVC_ORAMIGHUNTERSVC);
  castor::rtcopy::mighunter::IMigHunterSvc* oraSvc =
    dynamic_cast<castor::rtcopy::mighunter::IMigHunterSvc*>(dbSvc);

  // Throw an exception if the Oracle database service could not
  // be obtained
  if(oraSvc == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to get " << oraSvcName << " Oracle database service";
    throw(ex);
  }

  // For each service-class name
  for(
    std::list<std::string>::const_iterator svcClassName=m_listSvcClass.begin();
    svcClassName != m_listSvcClass.end();
    svcClassName++) {

    MigrationPolicyElementList infoCandidateTapeCopies;
    MigrationPolicyElementList candidatesToRestore;
    MigrationPolicyElementList invalidTapeCopies;

    std::map<u_signed64, std::list<castor::infoPolicy::MigrationPolicyElement> >      eligibleCandidates;

    try { // to catch exceptions specific of a svcclass

      u_signed64 initialSizeToTransfer=0;
      timeval    tvStart;
      gettimeofday(&tvStart, NULL);

      // Retreive tapecopy id and extra information from the db for the python
      // policy
      oraSvc->inputForMigrationPolicy((*svcClassName),
        &initialSizeToTransfer, infoCandidateTapeCopies);

      timeval tvEnd;
      gettimeofday(&tvEnd, NULL);
      signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsDb[] = {
        castor::dlf::Param("SvcClass", *svcClassName),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)};

      // Skip this service class if there are no candidate tape copies
      if (infoCandidateTapeCopies.empty()){
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NO_TAPECOPIES,
          paramsDb);
        continue; // For each service-class name
      } else {
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPECOPIES_FOUND,
          paramsDb);
      }

      // input for stream operation
      char* p=NULL;
      u_signed64 initialSizeCeiling = 10UL*1024UL*1024UL*1024UL; // 10G
      if ( (p = getconfent("MigHunter","SIZECEILING",0)) != NULL ) {
        initialSizeCeiling = strutou64(p);
      }

      gettimeofday(&tvStart, NULL);

      int ret = oraSvc->createOrUpdateStream((*svcClassName),
        initialSizeToTransfer, m_migrationDataThreshold, initialSizeCeiling,
        m_doClone, infoCandidateTapeCopies.size());

      // If the createOrUpdateStream() call failed
      if (ret<0){

        // Resurrect candidates
        oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);

        gettimeofday(&tvEnd, NULL);
        procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
          ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

        castor::dlf::Param paramsNoStreams[]= {
          castor::dlf::Param("SVCCLASS", *svcClassName),
          castor::dlf::Param("ProcessingTime", procTime * 0.000001)};

        // Log the error if it known on DLF for the stream
        switch(ret) {
        case -1:
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, NO_TAPEPOOLS,
            paramsNoStreams);
          break;
        case -2:
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, NOT_ENOUGH,
            paramsNoStreams);
          break;
        case -3:
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, NO_DRIVES,
            paramsNoStreams);
          break;
        }

        // Throw an exception
        castor::exception::Internal ex;

        ex.getMessage() <<
          "createOrUpdateStream() call failed"
          ": code=" << ex.code() <<
          ": message=" << ex.getMessage().str();

        throw ex;

      } // If the createOrUpdateStream() call failed

      // call the policy foreach tape copy

      u_signed64 policyYes=0;
      u_signed64 policyNo=0;
      u_signed64 withoutPolicy=0;

      gettimeofday(&tvStart, NULL);

      // For each infoCandidate
      for (
        MigrationPolicyElementList::iterator infoCandidate =
          infoCandidateTapeCopies.begin();
        infoCandidate != infoCandidateTapeCopies.end();
        infoCandidate++ ) {

        // add information from the Name Server
        try {

          getInfoFromNs(*svcClassName, *infoCandidate);

        } catch(castor::exception::Exception &e){

          // note that we've already logged something inside getInfoFromNS
          invalidTapeCopies.push_back(*infoCandidate);
          continue; // For each infoCandidate
        }

        struct Cns_fileid castorFileId;
        memset(&castorFileId,'\0',sizeof(castorFileId));
        strncpy(
          castorFileId.server,
          infoCandidate->nsHost().c_str(),
          sizeof(castorFileId.server)-1);

        castorFileId.fileid = infoCandidate->fileId();

        castor::dlf::Param paramsInput[] = {
          castor::dlf::Param("SvcClass", *svcClassName),
          castor::dlf::Param("tapecopy id", infoCandidate->tapeCopyId()),
          castor::dlf::Param("copy nb", infoCandidate->copyNb()),
          castor::dlf::Param("castorfilename",
            infoCandidate->castorFileName()),
          castor::dlf::Param("tapepoolname", infoCandidate->tapePoolName()),
          castor::dlf::Param("file mode", infoCandidate->fileMode()),
          castor::dlf::Param("nlink", infoCandidate->nlink()),
          castor::dlf::Param("uid", infoCandidate->uid()),
          castor::dlf::Param("gid", infoCandidate->gid()),
          castor::dlf::Param("fileSize", infoCandidate->fileSize()),
          castor::dlf::Param("aTime", infoCandidate->aTime()),
          castor::dlf::Param("mTime", infoCandidate->mTime()),
          castor::dlf::Param("cTime", infoCandidate->cTime()),
          castor::dlf::Param("fileClass", infoCandidate->fileClass()),
          castor::dlf::Param("status", infoCandidate->status()),
        };

        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, POLICY_INPUT,
          paramsInput, &castorFileId);

        castor::dlf::Param paramsOutput[] = {
          castor::dlf::Param("SvcClass", *svcClassName),
          castor::dlf::Param("tapecopy id", infoCandidate->tapeCopyId())};

        // policy called for each tape copy for each tape pool
        try {

          // Attach the tape copy if there is no policy
          if ( m_migrationPolicyDict == NULL ||
            infoCandidate->policyName().empty() ) {

            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
              START_WITHOUT_POLICY, paramsOutput, &castorFileId);

            eligibleCandidates[infoCandidate->tapePoolId()].push_back(
              *infoCandidate);

            withoutPolicy++;

          // Else apply the policy
          } else {

            // Apply the migration policy
            int policyResult = 0;
            try {
              ScopedPythonLock scopedPythonLoc;
              policyResult = applyMigrationPolicy(*infoCandidate);
            } catch(castor::exception::Exception &ex) {
              castor::exception::Exception ex2(ex.code());

              ex2.getMessage() <<
                "Failed to apply the migration policy"
                ": " << ex.getMessage();

              throw(ex2);
            }

            // Attach the tape copy if this is the result of applying the policy
            if (policyResult) {

              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                ALLOWED_BY_POLICY, paramsOutput, &castorFileId);

              eligibleCandidates[infoCandidate->tapePoolId()].push_back(
                *infoCandidate);

              policyYes++;

            // Else do not attach the tape copy
            } else {

              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NOT_ALLOWED,
                paramsOutput,&castorFileId);
              policyNo++;

            }
          } // Else apply the policy

        } catch (castor::exception::Exception &e) {
          struct Cns_fileid castorFileId;
          memset(&castorFileId,'\0',sizeof(castorFileId));
          strncpy(
            castorFileId.server,
            infoCandidate->nsHost().c_str(),
            sizeof(castorFileId.server)-1);
          castorFileId.fileid = infoCandidate->fileId();

          castor::dlf::Param params[] = {
            castor::dlf::Param("SvcClass", (*svcClassName)),
            castor::dlf::Param("policy", "Migration Policy"),
            castor::dlf::Param("code", sstrerror(e.code())),
            castor::dlf::Param("message", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR,
            params, &castorFileId);
          exit(-1);
        }
      } // For each infoCandidate

      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      // log in the dlf with the summary
      castor::dlf::Param paramsPolicy[] = {
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("allowed",withoutPolicy),
        castor::dlf::Param("allowedByPolicy",policyYes),
        castor::dlf::Param("notAllowed",policyNo),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, POLICY_RESULT,
        paramsPolicy);

      // For each eligibleCandidate
      for (
        std::map<
          u_signed64,
          std::list<castor::infoPolicy::MigrationPolicyElement>
        >::const_iterator eligibleCandidate = eligibleCandidates.begin();
        eligibleCandidate != eligibleCandidates.end();
        eligibleCandidate++) {

        try {
          //attach the eligible tape copies
          gettimeofday(&tvStart, NULL);

          oraSvc->attachTapeCopiesToStreams((*eligibleCandidate).second);

          gettimeofday(&tvEnd, NULL);
          procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
            ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);


          castor::dlf::Param paramsDbUpdate[] = {
            castor::dlf::Param("SvcClass",(*svcClassName)),
            castor::dlf::Param("tapepool", (*eligibleCandidate).first),
            castor::dlf::Param("ProcessingTime", procTime * 0.000001)};

          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
            ATTACHED_TAPECOPIES, paramsDbUpdate);

        } catch(castor::exception::Exception e) {
          castor::dlf::Param params[] = {
            castor::dlf::Param("SvcClass",(*svcClassName)),
            castor::dlf::Param("code", sstrerror(e.code())),
            castor::dlf::Param("message", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DB_ERROR, params);
          oraSvc->resurrectTapeCopies((*eligibleCandidate).second);
        }
      } // For each eligibleCandidate

      // resurrect tape copies not eligible
      gettimeofday(&tvStart, NULL);

      oraSvc->resurrectTapeCopies(candidatesToRestore);

      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsResurrect[] = {
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("nb tapecopies",candidatesToRestore.size()),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, RESURRECT_TAPECOPIES,
        paramsResurrect);

      // delete tapecopies which refers to files which are not in the nameserver

      gettimeofday(&tvStart, NULL);
      oraSvc->invalidateTapeCopies(invalidTapeCopies);

      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsInvalidate[] = {
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("nb tapecopies",invalidTapeCopies.size()),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, INVALIDATE_TAPECOPIES,
        paramsInvalidate);

    } catch (castor::exception::Exception e){
      // exception due to problems specific to the service class
    } catch (...) {
      // Do nothing
    }

    // added in the svclass loop

  } // For each service-class name
}


//------------------------------------------------------------------------------
// getInfoFromNs
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterThread::getInfoFromNs(
  const std::string                          &svcClassName,
  castor::infoPolicy::MigrationPolicyElement &elem) const
  throw (castor::exception::Exception) {

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
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USER_ERROR, NS_ERROR,
      paramsError, &cnsFile);

    castor::exception::Exception e(serrno);
    e.getMessage() <<
      "Cns_statx() call failed"
      ": " << sstrerror(serrno);
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


//------------------------------------------------------------------------------
// applyMigrationPolicy
//------------------------------------------------------------------------------
int castor::rtcopy::mighunter::MigHunterThread::applyMigrationPolicy(
  castor::infoPolicy::MigrationPolicyElement &elem)
  throw(castor::exception::Exception) {

  // Create the input tuple for the migration-policy Python-function
  //
  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)
  SmartPyObjectPtr inputObj(Py_BuildValue(
    (char*)"(s,s,K,K,K,K,K,K,K,K,K,K)",
    (elem.tapePoolName()).c_str(),
    (elem.castorFileName()).c_str(),
    elem.copyNb(),
    elem.fileId(),
    elem.fileSize(),
    elem.fileMode(),
    elem.uid(),
    elem.gid(),
    elem.aTime(),
    elem.mTime(),
    elem.cTime(),
    elem.fileClass()));

  // Get a pointer to the migration-policy Python-function
  const char *const functionName = elem.policyName().c_str();
  PyObject *pyFunc = PyDict_GetItemString((PyObject *)m_migrationPolicyDict,
    functionName);

  // Throw an exception if the migration-policy Python-function does not exist
  if(pyFunc == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Python function does not exist"
      ": functionName=" << functionName;

    throw ex;
  }

  // Throw an exception if the migration-policy Python-function is not callable
  if (!PyCallable_Check(pyFunc)) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Python function cannot be called"
      ": functionName=" << functionName;

    throw ex;
  }

  // Call the migration-policy Python-function
  SmartPyObjectPtr resultObj(PyObject_CallObject(pyFunc, inputObj.get()));

  // Throw an exception if the migration-policy Python-function call failed
  if(resultObj.get() == NULL) {
    castor::exception::Internal ex;

    ex.getMessage() <<
      "Failed to execute migration-policy Python-function" <<
      ": functionName=" <<  functionName;

    throw ex;
  }

  // Return the result of the Python function
  const int resultInt = PyInt_AsLong(resultObj.get());
  return resultInt;
}

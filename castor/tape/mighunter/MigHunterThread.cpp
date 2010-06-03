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
#include "castor/tape/python/python.hpp"

#include <list>
#include <stdlib.h>
#include <sys/types.h>
#include <u64subr.h>
#include <unistd.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include "castor/server/BaseThreadPool.hpp"

#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"

#include "castor/tape/mighunter/IMigHunterSvc.hpp"
#include "castor/tape/mighunter/MigHunterDlfMessageConstants.hpp"
#include "castor/tape/mighunter/MigHunterThread.hpp"
#include "castor/tape/mighunter/MigrationPolicyElement.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"

#include "castor/tape/utils/utils.hpp"

#include "h/Cns_api.h"
#include "h/getconfent.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mighunter::MigHunterThread::MigHunterThread(
  const std::list<std::string>             &svcClassList,
  const uint64_t                           migrationDataThreshold,
  const bool                               doClone,
  PyObject *const                          migrationPolicyDict,
  castor::tape::mighunter::MigHunterDaemon &daemon) throw() :

  m_listSvcClass(svcClassList),
  m_migrationDataThreshold(migrationDataThreshold),
  m_doClone(doClone),
  m_migrationPolicyDict(migrationPolicyDict),
  m_daemon(daemon) {

  if(migrationPolicyDict == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": migrationPolicyDict parameter is NULL");
  }
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::mighunter::MigHunterThread::run(void* arg) {

  try {

    exceptionThrowingRun(arg);

  } catch(castor::exception::Exception &ex) {

    // Log the exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);

  } catch(std::exception &ex) {

    // Log the exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.what())};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);

  } catch(...) {

    // Log the exception
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Uknown exception")};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);

  }
}


//------------------------------------------------------------------------------
// exceptionThrowingRun
//------------------------------------------------------------------------------
void castor::tape::mighunter::MigHunterThread::exceptionThrowingRun(void *arg) {

  server::BaseThreadPool *const threadPool = (server::BaseThreadPool*)arg;

  // Get a handle on the service to access the database
  const char *const oraSvcName = "OraMigHunterSvc";
  castor::IService* dbSvc =
    castor::BaseObject::services()->service(oraSvcName,
    castor::SVC_ORAMIGHUNTERSVC);
  castor::tape::mighunter::IMigHunterSvc* oraSvc =
    dynamic_cast<castor::tape::mighunter::IMigHunterSvc*>(dbSvc);

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

    u_signed64 notAllowedByPolicy   = 0;

    // The eligible canditates organized by tape pool so that the database
    // locking logic can take a lock on the tape pools one at a time
    std::map<u_signed64, std::list<MigrationPolicyElement> > eligibleCandidates;

    try { // to catch exceptions specific of a svcclass

      u_signed64 initialSizeToTransfer=0;
      timeval    tvStart;
      gettimeofday(&tvStart, NULL);

      try {
        // Retreive tape-copy ids and other information from the db which are
        // required by the migration-policy Python-function
        oraSvc->inputForMigrationPolicy((*svcClassName),
          &initialSizeToTransfer, infoCandidateTapeCopies);
      } catch(castor::exception::InvalidConfiguration &ex) {
        castor::dlf::Param params[] = {
        castor::dlf::Param("SVCCLASS", *svcClassName          ),
        castor::dlf::Param("error"   , "Invalid configuration"),
        castor::dlf::Param("Message", ex.getMessage().str()   ),
        castor::dlf::Param("Code"   , ex.code()               )};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

        m_daemon.shutdownGracefully(); // Non-blocking
        threadPool->shutdown(); // Non-blocking
        return; // run() will not be called again
      }

      timeval tvEnd;
      gettimeofday(&tvEnd, NULL);
      const signed64 inputForMigrationPolicyProcTime =
        ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsDb[] = {
        castor::dlf::Param("SvcClass", *svcClassName),
        castor::dlf::Param("ProcessingTime",
          inputForMigrationPolicyProcTime * 0.000001)};

      // Skip this service class if there are no candidate tape copies
      if (infoCandidateTapeCopies.empty()){
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NO_TAPECOPIES,
          paramsDb);

        castor::dlf::Param params[] = {
          castor::dlf::Param("SvcClass", *svcClassName)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
          MIGHUNTERTHREAD_SKIPPING_SRVCCLASS, params);

        continue; // For each service-class name
      } else {
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, TAPECOPIES_FOUND,
          paramsDb);
      }

      // input for stream operation
      char* p=NULL;
      u_signed64 initialSizeCeiling = 10UL*1024UL*1024UL*1024UL; // 10G
      if ( (p = getconfent("MIGHUNTER","SIZECEILING",0)) != NULL ) {
        initialSizeCeiling = strutou64(p);
      }

      int ret = oraSvc->createOrUpdateStream((*svcClassName),
        initialSizeToTransfer, m_migrationDataThreshold, initialSizeCeiling,
        m_doClone, infoCandidateTapeCopies.size());

      // If the createOrUpdateStream() call failed
      if (ret<0){

        // Resurrect candidates
        oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);

        castor::dlf::Param paramsNoStreams[]= {
          castor::dlf::Param("SVCCLASS", *svcClassName)};

        // Log the error if it known on DLF for the stream
        switch(ret) {
        case -1:
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, NO_TAPEPOOLS,
            paramsNoStreams);
          break;
        case -2:
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
            NOT_ENOUGH_DATA_TO_CREATE_STREAMS, paramsNoStreams);
          break;
        case -3:
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, NO_DRIVES,
            paramsNoStreams);
          break;
        }

        // Gracefully shutdown the daemon if the service class has no tape
        // pools, as this is an invalid configuration
        if(ret == -1) {
          castor::dlf::Param params[] = {
          castor::dlf::Param("SVCCLASS", *svcClassName),
          castor::dlf::Param("error"   ,
            "Invalid configuration: Service class has no tape pools")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
            GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

          m_daemon.shutdownGracefully(); // Non-blocking
          threadPool->shutdown(); // Non-blocking
          return; // run() will not be called again
        }

        // Gracefully shutdown the daemon if the nbDrives attribute of the
        // service class is set to 0, as this is an invalid configuration
        if(ret == -3) {
          castor::dlf::Param params[] = {
          castor::dlf::Param("SVCCLASS", *svcClassName),
          castor::dlf::Param("error"   ,
            "Invalid configuration: nbDrives set to 0")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
            GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

          m_daemon.shutdownGracefully(); // Non-blocking
          threadPool->shutdown(); // Non-blocking
          return; // run() will not be called again
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
          infoCandidate->nsHost.c_str(),
          sizeof(castorFileId.server)-1);

        castorFileId.fileid = infoCandidate->fileId;

        castor::dlf::Param paramsInput[] = {
          castor::dlf::Param("SvcClass", *svcClassName),
          castor::dlf::Param("tapecopy id", infoCandidate->tapeCopyId),
          castor::dlf::Param("copy nb", infoCandidate->copyNb),
          castor::dlf::Param("castorfilename",
            infoCandidate->castorFileName),
          castor::dlf::Param("tapepoolname", infoCandidate->tapePoolName),
          castor::dlf::Param("file mode", infoCandidate->fileMode),
          castor::dlf::Param("nlink", infoCandidate->nlink),
          castor::dlf::Param("uid", infoCandidate->uid),
          castor::dlf::Param("gid", infoCandidate->gid),
          castor::dlf::Param("fileSize", infoCandidate->fileSize),
          castor::dlf::Param("aTime", infoCandidate->aTime),
          castor::dlf::Param("mTime", infoCandidate->mTime),
          castor::dlf::Param("cTime", infoCandidate->cTime),
          castor::dlf::Param("fileClass", infoCandidate->fileClass),
          castor::dlf::Param("status", infoCandidate->status),
        };

        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
          MIGRATION_POLICY_INPUT, paramsInput, &castorFileId);

        // Gracefully shutdown the daemon if the migratorPolicy attribute of the
        // service class is an empty string, as this is an invalid configuration
        if(infoCandidate->policyName.empty()) {
          castor::dlf::Param params[] = {
          castor::dlf::Param("SVCCLASS"    , *svcClassName            ),
          castor::dlf::Param("tapecopy id" , infoCandidate->tapeCopyId),
          castor::dlf::Param("error"       ,
            "Invalid configuration: migratorPolicy is an empty string")};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
            GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

          oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);
          m_daemon.shutdownGracefully(); // Non-blocking
          threadPool->shutdown(); // Non-blocking
          return; // run() will not be called again
        }

        castor::dlf::Param paramsOutput[] = {
          castor::dlf::Param("SvcClass", *svcClassName),
          castor::dlf::Param("tapecopy id", infoCandidate->tapeCopyId)};

        // policy called for each tape copy for each tape pool
        try {

          // Get a lock on the embedded Python-interpreter
          castor::tape::python::ScopedPythonLock scopedPythonLock;

          // Try to get a handle on the migration-policy Python-function
          PyObject *const migrationPolicyFunc = python::getPythonFunction(
            m_migrationPolicyDict, infoCandidate->policyName.c_str());

          // Gracefully shutdown the daemon if the function does not exist in
          // the Python-module, as this is an invalid configuration
          if(migrationPolicyFunc == NULL) {
            castor::dlf::Param params[] = {
            castor::dlf::Param("SVCCLASS"    , *svcClassName              ),
            castor::dlf::Param("tapecopy id" , infoCandidate->tapeCopyId  ),
            castor::dlf::Param("error"       , "Invalid configuration:"
              " migratorPolicy function not found in Python-module"),
            castor::dlf::Param("functionName", infoCandidate->policyName  )};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
              GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

            oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);
            m_daemon.shutdownGracefully(); // Non-blocking
            threadPool->shutdown(); // Non-blocking
            return; // run() will not be called again
          }

          // Apply the migration policy
          int policyResult = 0;
          try {

            policyResult = applyMigrationPolicy(migrationPolicyFunc,
              *infoCandidate);

          } catch(castor::exception::Exception &ex) {

            // Gracefully shutdown the daemon if the migration policy could not
            // be applied
            castor::dlf::Param params[] = {
            castor::dlf::Param("SVCCLASS"    , *svcClassName              ),
            castor::dlf::Param("tapecopy id" , infoCandidate->tapeCopyId  ),
            castor::dlf::Param("error"       ,
              "Failed to apply migration policy"                          ),
            castor::dlf::Param("functionName", infoCandidate->policyName  ),
            castor::dlf::Param("Message"     , ex.getMessage().str()      ),
            castor::dlf::Param("Code"        , ex.code()                  )};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
              GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

            oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);
            m_daemon.shutdownGracefully(); // Non-blocking
            threadPool->shutdown(); // Non-blocking
            return; // run() will not be called again
          }

          // Attach the tape copy if this is the result of applying the policy
          if(policyResult) {

            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
              ALLOWED_BY_MIGRATION_POLICY, paramsOutput, &castorFileId);

            eligibleCandidates[infoCandidate->tapePoolId].push_back(
              *infoCandidate);

          // Else do not attach the tape copy
          } else {

            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
              NOT_ALLOWED_BY_MIGRATION_POLICY, paramsOutput,&castorFileId);
            notAllowedByPolicy++;

          }

        } catch (castor::exception::Exception &e) {
          // An exception here is fatal.  Log a message and exit
          struct Cns_fileid castorFileId;
          memset(&castorFileId,'\0',sizeof(castorFileId));
          strncpy(
            castorFileId.server,
            infoCandidate->nsHost.c_str(),
            sizeof(castorFileId.server)-1);
          castorFileId.fileid = infoCandidate->fileId;

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

      // log in the dlf with the summary
      castor::dlf::Param paramsPolicy[] = {
        castor::dlf::Param("SvcClass"          , (*svcClassName)          ),
        castor::dlf::Param("allowedByPolicy"   , eligibleCandidates.size()),
        castor::dlf::Param("notAllowedByPolicy", notAllowedByPolicy       )};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
        MIGRATION_POLICY_RESULT, paramsPolicy);

      try {
        checkEachTapeCopyWillBeAttachedOrInvalidated(
          infoCandidateTapeCopies, eligibleCandidates, invalidTapeCopies);
      } catch(castor::exception::TapeCopyNotFound &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("SVCCLASS"    , *svcClassName        ),
          castor::dlf::Param("tapecopy id" , ex.tapeCopyId()      ),
          castor::dlf::Param("code"        , sstrerror(ex.code()) ),
          castor::dlf::Param("message"     , ex.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

        oraSvc->resurrectTapeCopies(infoCandidateTapeCopies);
        m_daemon.shutdownGracefully(); // Non-blocking
        threadPool->shutdown(); // Non-blocking
        return; // run() will not be called again
      }

      // For each eligibleCandidate
      for (
        std::map<
          u_signed64,
          std::list<MigrationPolicyElement>
        >::const_iterator eligibleCandidate = eligibleCandidates.begin();
        eligibleCandidate != eligibleCandidates.end();
        eligibleCandidate++) {

        try {
          //attach the eligible tape copies
          gettimeofday(&tvStart, NULL);

          oraSvc->attachTapeCopiesToStreams((*eligibleCandidate).second);

          gettimeofday(&tvEnd, NULL);
          const signed64 attachTapeCopiesToStreamsProcTime =
            ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
            ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

          castor::dlf::Param paramsDbUpdate[] = {
            castor::dlf::Param("SvcClass",(*svcClassName)),
            castor::dlf::Param("tapepool", (*eligibleCandidate).first),
            castor::dlf::Param("ProcessingTime",
              attachTapeCopiesToStreamsProcTime * 0.000001)};

          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
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
      const signed64 resurrectTapeCopiesProcTime =
        ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsResurrect[] = {
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("nb tapecopies",candidatesToRestore.size()),
        castor::dlf::Param("ProcessingTime",
          resurrectTapeCopiesProcTime * 0.000001)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, RESURRECT_TAPECOPIES,
        paramsResurrect);

      // delete tapecopies which refers to files which are not in the nameserver
      gettimeofday(&tvStart, NULL);

      oraSvc->invalidateTapeCopies(invalidTapeCopies);

      gettimeofday(&tvEnd, NULL);
      const signed64 invalidateTapeCopiesProcTime =
        ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsInvalidate[] = {
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("nb tapecopies",invalidTapeCopies.size()),
        castor::dlf::Param("ProcessingTime",
          invalidateTapeCopiesProcTime * 0.000001)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, INVALIDATE_TAPECOPIES,
        paramsInvalidate);

    } catch (castor::exception::Exception e){
      // exception due to problems specific to the service class
    } catch (...) {
      // Do nothing
    }

    // Log summary
    castor::dlf::Param paramsPolicy[] = {
      castor::dlf::Param("SvcClass"          , (*svcClassName)           ),
      castor::dlf::Param("allowedByPolicy"   , eligibleCandidates.size() ),
      castor::dlf::Param("notAllowedByPolicy", notAllowedByPolicy        ),
      castor::dlf::Param("resurrected"       , candidatesToRestore.size()),
      castor::dlf::Param("invalidated"       , invalidTapeCopies.size()  )};

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
      MIGRATION_POLICY_RESULT, paramsPolicy);

  } // For each service-class name
}


//------------------------------------------------------------------------------
// getInfoFromNs
//------------------------------------------------------------------------------
void castor::tape::mighunter::MigHunterThread::getInfoFromNs(
  const std::string &svcClassName,
  MigrationPolicyElement &elem) const
  throw (castor::exception::Exception) {

  struct Cns_filestat statbuf;
  struct Cns_fileid cnsFile;
  memset(&cnsFile,'\0',sizeof(cnsFile));
  cnsFile.fileid=elem.fileId;
  char castorFileName[CA_MAXPATHLEN+1];
  castorFileName[0] = '\0';
  strncpy(cnsFile.server,elem.nsHost.c_str(),sizeof(cnsFile.server)-1);

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

  elem.fileId=statbuf.fileid;
  elem.fileMode=(u_signed64)statbuf.filemode;
  elem.nlink=statbuf.nlink;
  elem.uid=(u_signed64)statbuf.uid;
  elem.gid=(u_signed64)statbuf.gid;
  elem.fileSize=statbuf.filesize;
  elem.aTime=(u_signed64)statbuf.atime;
  elem.mTime=(u_signed64)statbuf.mtime;
  elem.cTime=(u_signed64)statbuf.ctime;
  elem.fileClass=(int)statbuf.fileclass;
  elem.status=(unsigned char)statbuf.status;
}


//------------------------------------------------------------------------------
// applyMigrationPolicy
//------------------------------------------------------------------------------
int castor::tape::mighunter::MigHunterThread::applyMigrationPolicy(
  PyObject *const                                 pyFunc,
  castor::tape::mighunter::MigrationPolicyElement &elem)
  throw(castor::exception::Exception) {

  if(pyFunc == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
     ": pyFunc parameter is NULL");
  }

  // Create the input tuple for the migration-policy Python-function
  //
  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)
  castor::tape::python::SmartPyObjectPtr inputObj(Py_BuildValue(
    (char*)"(s,s,K,K,K,K,K,K,K,K,K,K)",
    (elem.tapePoolName).c_str(),
    (elem.castorFileName).c_str(),
    elem.copyNb,
    elem.fileId,
    elem.fileSize,
    elem.fileMode,
    elem.uid,
    elem.gid,
    elem.aTime,
    elem.mTime,
    elem.cTime,
    elem.fileClass));

  // Call the migration-policy Python-function
  castor::tape::python::SmartPyObjectPtr resultObj(PyObject_CallObject(pyFunc,
    inputObj.get()));

  // Throw an exception if the migration-policy Python-function call failed
  if(resultObj.get() == NULL) {

    // Try to determine the Python exception if there was aPython error
    PyObject *const pyEx = PyErr_Occurred();
    const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

    // Clear the Python error if there was one
    if(pyEx != NULL) {
      PyErr_Clear();
    }

    castor::exception::Internal ex;

    ex.getMessage() <<
      "Failed to execute migration-policy Python-function" <<
      ": functionName=" << elem.policyName.c_str() <<
      ": pythonException=" << pyExStr;

    throw ex;
  }

  // Return the result of the Python function
  return PyInt_AsLong(resultObj.get());
}


//------------------------------------------------------------------------------
// checkEachTapeCopyWillBeAttachedOrInvalidated
//------------------------------------------------------------------------------
void castor::tape::mighunter::MigHunterThread::
  checkEachTapeCopyWillBeAttachedOrInvalidated(
  const MigrationPolicyElementList &tapeCopiesToBeFound,
  const std::map<u_signed64, std::list<MigrationPolicyElement> >
    &tapePool2TapeCopies,
  const MigrationPolicyElementList &invalidTapeCopies)
  throw(castor::exception::TapeCopyNotFound) {

  // For each tape-copy to be checked
  for(
    MigrationPolicyElementList::const_iterator tapeCopyToBeFound =
      tapeCopiesToBeFound.begin();
    tapeCopyToBeFound != tapeCopiesToBeFound.end();
    tapeCopyToBeFound++ ) {

    if(!tapeCopyIsInList(*tapeCopyToBeFound, invalidTapeCopies)) {
      if(!tapeCopyIsInMapOfTapePool2TapeCopies(*tapeCopyToBeFound,
        tapePool2TapeCopies)) {
        castor::exception::TapeCopyNotFound ex(tapeCopyToBeFound->tapeCopyId);

        ex.getMessage() <<
          "Tape-copy not found in the tape-copies to be attached to streams"
          ": Coverage of the migrationPolicy function is not complete" <<
          ": functionName=" << tapeCopyToBeFound->policyName;

        throw ex;
      }
    }
  }
}


bool castor::tape::mighunter::MigHunterThread::
  tapeCopyIsInMapOfTapePool2TapeCopies(
  const MigrationPolicyElement &tapeCopyToBeFound,
  const std::map<u_signed64, MigrationPolicyElementList> &tapePool2TapeCopies)
  throw() {

  // For each list of tape-copies to be attached to a tape-pool
  for (
    std::map<u_signed64, MigrationPolicyElementList>::const_iterator
      tapePoolTapeCopies = tapePool2TapeCopies.begin();
    tapePoolTapeCopies != tapePool2TapeCopies.end();
    tapePoolTapeCopies++) {

    if(tapeCopyIsInList(tapeCopyToBeFound, tapePoolTapeCopies->second)) {
      return true;
    }
  }

  return false;
}


bool castor::tape::mighunter::MigHunterThread::tapeCopyIsInList(
  const MigrationPolicyElement &tapeCopyToBeFound,
  const MigrationPolicyElementList &tapeCopies) throw() {

  for (
    MigrationPolicyElementList::const_iterator tapeCopyInList =
      tapeCopies.begin();
    tapeCopyInList != tapeCopies.end();
    tapeCopyInList++ ) {

    if(tapeCopyToBeFound.tapeCopyId == tapeCopyInList->tapeCopyId) {
      return true;
    }
  }

  return false;
}

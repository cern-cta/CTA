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

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include "castor/tape/python/python.hpp"

#include <list>
#include <sys/types.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include "castor/stager/SvcClass.hpp"

#include "castor/tape/mighunter/IMigHunterSvc.hpp"
#include "castor/tape/mighunter/MigHunterDlfMessageConstants.hpp"
#include "castor/tape/mighunter/StreamPolicyElement.hpp"
#include "castor/tape/mighunter/StreamThread.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"

#include "castor/tape/utils/utils.hpp"

#include "h/Cns_api.h"
#include "h/getconfent.h"
#include "h/u64subr.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mighunter::StreamThread::StreamThread(
  const std::list<std::string> &svcClassArray,
  PyObject *const              streamPolicyDict,
  const char *const            streamPolicyModuleStatus) throw() :
  m_listSvcClass(svcClassArray),
  m_streamPolicyDict(streamPolicyDict),
  m_streamPolicyModuleStatus(streamPolicyModuleStatus) {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::run(void *arg) {

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
      castor::dlf::Param("Message", "Unknown exception")};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);

  }
}


//------------------------------------------------------------------------------
// exceptionThrowingRun
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::exceptionThrowingRun(void *arg) {

  // Get a handle on the service to access the database
  const char *const oraSvcName = "OraMigHunterSvc";
  castor::IService* dbSvc = castor::BaseObject::services()->service(oraSvcName,
    castor::SVC_ORAMIGHUNTERSVC);
  castor::tape::mighunter::IMigHunterSvc* oraSvc = 
    dynamic_cast<castor::tape::mighunter::IMigHunterSvc*>(dbSvc);

  // Throw an exception if the Oracle database service could not
  // be obtained
  if (oraSvc == NULL) {
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

    StreamPolicyElementList infoCandidateStreams;
    StreamPolicyElementList eligibleStreams;
    StreamPolicyElementList streamsToRestore;

    try { // to catch exceptions specific of a svcclass

      // Retrieve information from the db to know which stream should be
      // started and attach the eligible tapecopy

      timeval tvStart;
      gettimeofday(&tvStart, NULL);

      oraSvc->inputForStreamPolicy(*svcClassName,infoCandidateStreams);

      timeval tvEnd;
      gettimeofday(&tvEnd, NULL);
      signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) -
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsDb[] = {
        castor::dlf::Param("SvcClass", *svcClassName),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)};

      // Skip this service class if there are no candidate streams
      if (infoCandidateStreams.empty()) {
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NO_STREAM, paramsDb);
        continue; // For each service-class name
      } else {
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, STREAMS_FOUND,
        paramsDb);
      }

      // call the policy for the different stream
      u_signed64 nbDrives=0;
      u_signed64 runningStreams=0;

      if (infoCandidateStreams.begin() != infoCandidateStreams.end()){

        // the following information are always the same in all the
        // candidates for the same svcclass
        nbDrives=infoCandidateStreams.begin()->maxNumStreams;
        runningStreams=infoCandidateStreams.begin()->runningStream;
      }

      // counters for logging
      u_signed64 nbAllowedByPolicy      = 0;
      u_signed64 nbNotAllowedByPolicy   = 0;
      u_signed64 nbAllowedWithoutPolicy = 0;

      gettimeofday(&tvStart, NULL);

      // For each infoCandidateStream
      for (
        StreamPolicyElementList::iterator infoCandidateStream =
          infoCandidateStreams.begin();
         infoCandidateStream != infoCandidateStreams.end();
         infoCandidateStream++) {
 
        // Get new potential value
        infoCandidateStream->runningStream = runningStreams;

        // If there are no candidates then there is no point to call the policy,
        // therefore skip this infoCandidateStream
        if (infoCandidateStream->numBytes==0) {
          streamsToRestore.push_back(*infoCandidateStream);
          continue; // For each infoCandidateStream
        }

        castor::dlf::Param paramsInput[] = {
          castor::dlf::Param("SvcClass", *svcClassName),
          castor::dlf::Param("stream id",
            infoCandidateStream->streamId),
          castor::dlf::Param("running streams",
            infoCandidateStream->runningStream),
          castor::dlf::Param("bytes attached",
            infoCandidateStream->numBytes),
          castor::dlf::Param("number of files",
            infoCandidateStream->numFiles),
          castor::dlf::Param("max number of streams", 
            infoCandidateStream->maxNumStreams),
          castor::dlf::Param("age", infoCandidateStream->age)};

        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, STREAM_INPUT, 
          paramsInput);

        castor::dlf::Param paramsOutput[] = {
          castor::dlf::Param("SvcClass", *svcClassName),
          castor::dlf::Param("stream id", infoCandidateStream->streamId)};

        // policy called for each tape copy for each tape pool
        try {

          // Get a lock on the embedded Python-interpreter
          castor::tape::python::ScopedPythonLock scopedPythonLock;

          // Try to get a handle on the stream-policy Python-function
          PyObject *streamPolicyFunc = NULL;
          if(m_streamPolicyDict != NULL &&
            !infoCandidateStream->policyName.empty()) {

            streamPolicyFunc = python::getPythonFunction(
              m_streamPolicyDict, infoCandidateStream->policyName.c_str());

            // Log an alert if the function does not exist in the Python-module
            if(streamPolicyFunc == NULL) {
              castor::dlf::Param params[] = {
                castor::dlf::Param("SvcClass",*svcClassName),
                castor::dlf::Param("stream id",infoCandidateStream->streamId),
                castor::dlf::Param("functionName",infoCandidateStream->policyName)};

              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT,
                STREAM_POLICY_FUNCTION_NOT_IN_MODULE, params);
            }
          }

          // Attach the stream if there is no stream-policy Python-function
          if(streamPolicyFunc == NULL) {

            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
              START_WITHOUT_POLICY, paramsOutput);

            eligibleStreams.push_back(*infoCandidateStream);
            runningStreams++;
            nbAllowedWithoutPolicy++;

          // Else apply the policy
          } else {

            // Apply stream the policy
            int policyResult = 0;
            try {

              policyResult = applyStreamPolicy(streamPolicyFunc,
                *infoCandidateStream);

            } catch(castor::exception::Exception &ex) {

              castor::dlf::Param params[] = {
                castor::dlf::Param("code"   , sstrerror(ex.code()) ),
                castor::dlf::Param("message", ex.getMessage().str())};
              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
                FAILED_TO_APPLY_STREAM_POLICY, params);

              // Treat an exception in the same way as there being no policy,
              // i.e. attach the stream
              eligibleStreams.push_back(*infoCandidateStream);
              runningStreams++;
              nbAllowedWithoutPolicy++;

              // Skip to the next canditate
              continue; // For each infoCandidateStream
            }

            // Start the stream if this is the result of applying the policy
            if (policyResult) {

              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, 
                ALLOWED_BY_STREAM_POLICY, paramsOutput);

              eligibleStreams.push_back(*infoCandidateStream);

              runningStreams++;
              nbAllowedByPolicy++;

            // Else do not start the stream
            } else {

              castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                NOT_ALLOWED_BY_STREAM_POLICY, paramsOutput);
              streamsToRestore.push_back(*infoCandidateStream);
              nbNotAllowedByPolicy++;

            }
          }

        } catch (castor::exception::Exception &e) {
          // An exception here is fatal.  Log a message and exit
          castor::dlf::Param params[] = {
            castor::dlf::Param("policy","Stream Policy"),
            castor::dlf::Param("code", sstrerror(e.code())),
            castor::dlf::Param("stream",infoCandidateStream->streamId),
            castor::dlf::Param("message", e.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR,
             params);
          exit(-1);
        }

      } // For each infoCandidateStream

      gettimeofday(&tvEnd, NULL);
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - 
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      // log in the dlf with the summary
      castor::dlf::Param paramsPolicy[] = {
        castor::dlf::Param("SvcClass"            , (*svcClassName)           ),
        castor::dlf::Param("policyModuleStatus"  , m_streamPolicyModuleStatus),
        castor::dlf::Param("allowedWithoutPolicy", nbAllowedWithoutPolicy    ),
        castor::dlf::Param("allowedByPolicy"     , nbAllowedByPolicy         ),
        castor::dlf::Param("notAllowedByPolicy"  , nbNotAllowedByPolicy      ),
        castor::dlf::Param("runningStreams"      , runningStreams            ),
        castor::dlf::Param("ProcessingTime"      , procTime * 0.000001       )};

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STREAM_POLICY_RESULT,
         paramsPolicy);

      gettimeofday(&tvStart, NULL);

      // start streams

      oraSvc->startChosenStreams(eligibleStreams);
      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - 
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsStart[]={
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STARTED_STREAMS,
         paramsStart);

      // stop streams

      gettimeofday(&tvStart, NULL);

      oraSvc->stopChosenStreams(streamsToRestore);

      gettimeofday(&tvEnd, NULL);
      procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - 
        ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);

      castor::dlf::Param paramsStop[]={
        castor::dlf::Param("SvcClass",(*svcClassName)),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STOP_STREAMS,
        paramsStop);

    } catch (castor::exception::Exception e){
        // exception due to problems specific to the service class
    } catch (...){
      // Do nothing
    }

  } // loop for svcclass
}


//------------------------------------------------------------------------------
// applyStreamPolicy
//------------------------------------------------------------------------------
int castor::tape::mighunter::StreamThread::applyStreamPolicy(
  PyObject *const                              pyFunc,
  castor::tape::mighunter::StreamPolicyElement &elem)
  throw(castor::exception::Exception) {

  if(pyFunc == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
     ": pyFunc parameter is NULL");
  }
  
  // Create the input tuple for the stream-policy Python-function
  //
  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)
  castor::tape::python::SmartPyObjectPtr inputObj(Py_BuildValue(
    (char *)"(K,K,K,K,K)",
    elem.runningStream,
    elem.numFiles,
    elem.numBytes,
    elem.maxNumStreams,
    elem.age));

  // Call the stream-policy Python-function
  castor::tape::python::SmartPyObjectPtr resultObj(PyObject_CallObject(pyFunc,
    inputObj.get()));

  // Throw an exception if the stream-policy Python-function call failed
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
      "Failed to execute stream-policy Python-function" <<
      ": functionName=" <<  elem.policyName.c_str() <<
      ": pythonException=" << pyExStr;

    throw ex;
  }

  // Return the result of the Python function
  return PyInt_AsLong(resultObj.get());
}

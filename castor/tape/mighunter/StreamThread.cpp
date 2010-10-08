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
 *
 *
 *
 * @author Steven.Murray@cern.ch
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

#include "castor/server/BaseThreadPool.hpp"

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
  const std::list<std::string>             &svcClassArray,
  PyObject                          *const inspectGetargspecFunc,
  PyObject                          *const streamPolicyDict,
  castor::tape::mighunter::MigHunterDaemon &daemon)
  throw(castor::exception::Exception) :
  m_listSvcClass(svcClassArray),
  m_inspectGetargspecFunc(inspectGetargspecFunc),
  m_streamPolicyDict(streamPolicyDict),
  m_daemon(daemon) {

  if(inspectGetargspecFunc == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": inspectGetargspecFunc parameter is NULL");
  }

  if(streamPolicyDict == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": streamPolicyDict parameter is NULL");
  }

  // Set the argument names a stream-policy Python-function must have in order
  // to be considered valid
  m_expectedStreamPolicyArgNames.push_back("streamId");
  m_expectedStreamPolicyArgNames.push_back("numTapeCopies");
  m_expectedStreamPolicyArgNames.push_back("totalBytes");
  m_expectedStreamPolicyArgNames.push_back("ageOfOldestTapeCopy");
  m_expectedStreamPolicyArgNames.push_back("tapePoolId");
  m_expectedStreamPolicyArgNames.push_back("tapePoolName");
  m_expectedStreamPolicyArgNames.push_back("tapePoolNbRunningStreams");
  m_expectedStreamPolicyArgNames.push_back("svcClassId");
  m_expectedStreamPolicyArgNames.push_back("svcClassName");
  m_expectedStreamPolicyArgNames.push_back("svcClassNbDrives");
  m_expectedStreamPolicyArgNames.push_back("svcClassNbRunningStreams");
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::run(void *arg) {

 // Run the code of the thread, logging any raised exceptions
 try {
    exceptionThrowingRun(arg);
  } catch(castor::exception::Exception& ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);
  } catch(std::exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.what())};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);
  } catch(...) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Unknown exception")};
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, params);
  }
}


//------------------------------------------------------------------------------
// exceptionThrowingRun
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::exceptionThrowingRun(
  void *const arg) {

  server::BaseThreadPool *const threadPool = (server::BaseThreadPool*)arg;

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

  // Apply the stream-policy for each service-class managed by this
  // daemon
  for(std::list<std::string>::const_iterator svcClassNameItor =
    m_listSvcClass.begin(); svcClassNameItor != m_listSvcClass.end();
    svcClassNameItor++) {

    const std::string &svcClassName = *svcClassNameItor;

    try {
      applyStreamPolicyToSvcClass(oraSvc, svcClassName);
    } catch(castor::exception::InvalidConfiguration& ex) {
      // Gracefully shutdown the daemon in the event of an invalid configuration
      castor::dlf::Param params[] = {
        castor::dlf::Param("SvcClass", svcClassName           ),
        castor::dlf::Param("error"   , "Invalid configuration"),
        castor::dlf::Param("Message" , ex.getMessage().str()  ),
        castor::dlf::Param("Code"    , ex.code()              )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
        GRACEFUL_SHUTDOWN_DUE_TO_ERROR, params);

      m_daemon.shutdownGracefully(); // Non-blocking
      threadPool->shutdown(); // Non-blocking
      return; // run() will not be called again
    } catch(castor::exception::Exception& ex) {
      // Log an error and continue
      castor::dlf::Param params[] = {
        castor::dlf::Param("SvcClass", svcClassName         ),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
        APPLY_STREAM_POLICY_TO_SVCCLASS_EXCEPT, params);
    } catch(...) {
      // Log an error and continue
      castor::dlf::Param params[] = {
        castor::dlf::Param("SvcClass", svcClassName       ),
        castor::dlf::Param("Message" , "Unknown exception")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 
        APPLY_STREAM_POLICY_TO_SVCCLASS_EXCEPT, params);
    }
  }
}


//------------------------------------------------------------------------------
// applyStreamPolicyToSvcClass
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::applyStreamPolicyToSvcClass(
  castor::tape::mighunter::IMigHunterSvc *const oraSvc,
  const std::string                             &svcClassName)
  throw(castor::exception::InvalidConfiguration, castor::exception::Exception) {

  // For the service-class specified by svcClassName, get the service-class
  // database ID, the stream-policy name and the list of candidate streams to
  // be passed to the stream-policy
  u_signed64 svcClassId = 0;
  std::string streamPolicyName;
  u_signed64 svcClassNbDrives = 0;
  StreamForStreamPolicyList streamsForPolicy;
  {
    timeval start, end;
    gettimeofday(&start, NULL);
    oraSvc->streamsForStreamPolicy(svcClassName, svcClassId, streamPolicyName,
      svcClassNbDrives, streamsForPolicy);
    gettimeofday(&end, NULL);
    signed64 procTime = ((end.tv_sec * 1000000) + end.tv_usec) -
      ((start.tv_sec * 1000000) + start.tv_usec);

    if(streamsForPolicy.empty()) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("SvcClass"      , svcClassName       ),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, NO_STREAM, params);
    } else {
      castor::dlf::Param params[] = {
        castor::dlf::Param("SvcClass"      , svcClassName           ),
        castor::dlf::Param("ProcessingTime", procTime * 0.000001    ),
        castor::dlf::Param("nbStreams"     , streamsForPolicy.size())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, STREAMS_FOUND, params);
    }
  }

  // If the stream-policy function name has not been set then throw an
  // invalid-configuration exception to cause this daemon to gracefully
  // shutdown
  if(streamPolicyName.empty()) {
    castor::exception::InvalidConfiguration ex;

    ex.getMessage() <<
      "Failed to apply stream-policy to service-class"
      ": Invalid configuration"
      ": The streamPolicy attribute of the service-class is not set";

    throw(ex);
  }

  // Try to get a handle on the stream-policy Python-function
  PyObject *const streamPolicyPyFunc = python::getPythonFunctionWithLock(
    m_streamPolicyDict, streamPolicyName.c_str());

  // If the function does not exist in the Python-module, then throw an
  // invalid-configuration exception to cause this daemon to gracefully
  // shutdown
  if(streamPolicyPyFunc == NULL) {
    castor::exception::InvalidConfiguration ex;
    ex.getMessage() <<
      "Failed to apply stream-policy to service-class"
      ": Invalid configuration"
      ": Failed to get a handle on the stream-policy Python-function"
      ": streamPolicy function not found in Python-module"
      ": streamPolicyName=" << streamPolicyName;

    throw(ex);
  }

  // Throw an InvalidConfiguration exception if the signature of the
  // stream-policy Python-function is incorrect
  checkStreamPolicyArgNames(streamPolicyName, streamPolicyPyFunc);

  // Return if there are no streams for the stream-policy.
  //
  // This return is purposely made after the check of the signature of the
  // stream-policy Python-function, because invalid configurations should be
  // detected as soon as possible.
  if(streamsForPolicy.empty()) {
    return;
  }

  // Get the database ID, name and number of running streams for each of the
  // tape-pools associated with the service-class
  IdToTapePoolForStreamPolicyMap tapePoolsForPolicy;
  {
    timeval start, end;
    gettimeofday(&start, NULL);
    oraSvc->tapePoolsForStreamPolicy(svcClassId, tapePoolsForPolicy);
    gettimeofday(&end, NULL);
    signed64 procTime = ((end.tv_sec * 1000000) + end.tv_usec) -
      ((start.tv_sec * 1000000) + start.tv_usec);

    castor::dlf::Param params[] = {
      castor::dlf::Param("SvcClass"      , svcClassName             ),
      castor::dlf::Param("svcClassId"    , svcClassId               ),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001      ),
      castor::dlf::Param("nbTapePools"   , tapePoolsForPolicy.size())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
      GOT_TAPE_POOLS_FOR_STREAM_POLICY, params);
  }

  // Call the stream-policy for each stream and log a summary
  std::list<u_signed64> streamsAcceptedByPolicy;
  std::list<u_signed64> streamsRejectedByPolicy;
  std::list<u_signed64> streamsNoTapeCopies;
  {
    timeval start, end;
    gettimeofday(&start, NULL);
    callStreamPolicyForEachStream(svcClassId, svcClassName, svcClassNbDrives,
      streamPolicyName, streamPolicyPyFunc, streamsForPolicy,
      tapePoolsForPolicy, streamsAcceptedByPolicy, streamsRejectedByPolicy,
      streamsNoTapeCopies);
    gettimeofday(&end, NULL);
    signed64 procTime = ((end.tv_sec * 1000000) + end.tv_usec) -
      ((start.tv_sec * 1000000) + start.tv_usec);

    const bool allProcessed = streamsForPolicy.size() ==
      streamsAcceptedByPolicy.size() + streamsRejectedByPolicy.size() +
      streamsNoTapeCopies.size();
    const char *const allProcessedStr = allProcessed ? "TRUE" : "FALSE";

    castor::dlf::Param paramsPolicy[] = {
      castor::dlf::Param("SvcClass"          , svcClassName                  ),
      castor::dlf::Param("nbStreamsForPolicy", streamsForPolicy.size()       ),
      castor::dlf::Param("nbAcceptedByPolicy", streamsAcceptedByPolicy.size()),
      castor::dlf::Param("nbRejectedByPolicy", streamsRejectedByPolicy.size()),
      castor::dlf::Param("nbNoTapeCopies"    , streamsNoTapeCopies.size()    ),
      castor::dlf::Param("allProcessed"      , allProcessedStr               ),
      castor::dlf::Param("ProcessingTime"    , procTime * 0.000001           )};

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STREAM_POLICY_RESULT,
      paramsPolicy);
  }

  // Start the streams that were accepted by the stream-policy
  if(!streamsAcceptedByPolicy.empty()) {
    timeval start, end;
    gettimeofday(&start, NULL);
    oraSvc->startChosenStreams(streamsAcceptedByPolicy);
    gettimeofday(&end, NULL);
    signed64 procTime = ((end.tv_sec * 1000000) + end.tv_usec) -
      ((start.tv_sec * 1000000) + start.tv_usec);

    castor::dlf::Param paramsStart[]={
      castor::dlf::Param("SvcClass"      , svcClassName                  ),
      castor::dlf::Param("nbStreams"     , streamsAcceptedByPolicy.size()),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001           )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STARTED_STREAMS,
      paramsStart);
  }

  // Create the list of streams that should be deleted or stopped.  This list
  // is the concatentation of the list of streams rejected by the stream-policy
  // and the list of streams with no tape-copies attached.
  std::list<u_signed64> streamsToBeDeletedOrStopped(streamsRejectedByPolicy);
  streamsToBeDeletedOrStopped.insert(streamsToBeDeletedOrStopped.end(),
    streamsNoTapeCopies.begin(), streamsNoTapeCopies.end());

  // Delete or stop streams that should be deleted or stopped
  if(!streamsToBeDeletedOrStopped.empty()) {
    timeval start, end;
    gettimeofday(&start, NULL);
    oraSvc->stopChosenStreams(streamsToBeDeletedOrStopped);
    gettimeofday(&end, NULL);
    signed64 procTime = ((end.tv_sec * 1000000) + end.tv_usec) -
      ((start.tv_sec * 1000000) + start.tv_usec);

    castor::dlf::Param paramsStop[]={
      castor::dlf::Param("SvcClass"      , svcClassName                      ),
      castor::dlf::Param("nbStreams"     , streamsToBeDeletedOrStopped.size()),
      castor::dlf::Param("ProcessingTime", procTime * 0.000001               )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, STOP_STREAMS,
      paramsStop);
  }
}


//------------------------------------------------------------------------------
// checkStreamPolicyArgNames
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::checkStreamPolicyArgNames(
  const std::string &streamPolicyName,
  PyObject   *const streamPolicyPyFunc)
  throw(castor::exception::InvalidConfiguration) {

  // If the policy-function has already been found to be valid, then return
  if(m_namesOfValidPolicyFuncs.find(streamPolicyName) !=
    m_namesOfValidPolicyFuncs.end()) {
    return;
  }

  python::checkFuncArgNames(streamPolicyName, m_expectedStreamPolicyArgNames,
    m_inspectGetargspecFunc, streamPolicyPyFunc);

  // Remember the policy-function is valid
  m_namesOfValidPolicyFuncs.insert(streamPolicyName);
}


//------------------------------------------------------------------------------
// callStreamPolicyForEachStream
//------------------------------------------------------------------------------
void castor::tape::mighunter::StreamThread::callStreamPolicyForEachStream(
  const u_signed64                svcClassId,
  const std::string               &svcClassName,
  const u_signed64                svcClassNbDrives,
  const std::string               &streamPolicyName,
  PyObject *const                 streamPolicyPyFunc,
  const StreamForStreamPolicyList &streamsForPolicy,
  IdToTapePoolForStreamPolicyMap  &tapePoolsForPolicy,
  std::list<u_signed64>           &streamsAcceptedByPolicy,
  std::list<u_signed64>           &streamsRejectedByPolicy,
  std::list<u_signed64>           &streamsNoTapeCopies)
  throw(castor::exception::Exception) {

  // For each candidate stream
  for(StreamForStreamPolicyList::const_iterator streamForPolicyItor =
    streamsForPolicy.begin(); streamForPolicyItor != streamsForPolicy.end();
    streamForPolicyItor++) {

    const StreamForStreamPolicy &streamForPolicy = *streamForPolicyItor;

    // If the stream has no tape-copies, then push it on to the list of streams
    // with no tape-copies
    if(streamForPolicy.numTapeCopies == 0) {
      streamsNoTapeCopies.push_back(streamForPolicy.streamId);

    } else {

      // Call the stream-policy Python-function, updating the number of running
      // streams per tape-pool if the stream is accepted
      const bool streamAcceptedByPolicy = callStreamPolicyPyFuncForStream(
        svcClassId, svcClassName, svcClassNbDrives, streamPolicyName,
        streamPolicyPyFunc, streamForPolicy, tapePoolsForPolicy);

      // Push the stream accordingly onto either the list of streams accepted
      // by the policy or the list of streams rejected by the policy
      if(streamAcceptedByPolicy) {
        streamsAcceptedByPolicy.push_back(streamForPolicy.streamId);
      } else {
        streamsRejectedByPolicy.push_back(streamForPolicy.streamId);
      }
    }
  }
}


//------------------------------------------------------------------------------
// callStreamPolicyPyFuncForStream
//------------------------------------------------------------------------------
bool castor::tape::mighunter::StreamThread::callStreamPolicyPyFuncForStream(
  const u_signed64               svcClassId,
  const std::string              &svcClassName,
  const u_signed64               svcClassNbDrives,
  const std::string              &streamPolicyName,
  PyObject *const                streamPolicyPyFunc,
  const StreamForStreamPolicy    &streamForPolicy,
  IdToTapePoolForStreamPolicyMap &tapePoolsForPolicy)
  throw(castor::exception::Exception) {

  // Get a handle on the tape-pool of the stream
  IdToTapePoolForStreamPolicyMap::iterator tapePoolItor =
    tapePoolsForPolicy.find(streamForPolicy.tapePoolId);

  // If the tape-pool of the stream could not be found, then log an error
  // message and reject the stream
  if(tapePoolItor == tapePoolsForPolicy.end()) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("SvcClass"           , svcClassName                 ),
      castor::dlf::Param("streamId"           , streamForPolicy.streamId     ),
      castor::dlf::Param("numTapeCopies"      , streamForPolicy.numTapeCopies),
      castor::dlf::Param("totalBytes"         , streamForPolicy.totalBytes   ),
      castor::dlf::Param("ageOfOldestTapeCopy",
        streamForPolicy.ageOfOldestTapeCopy                                  ),
      castor::dlf::Param("tapePoolId"         , streamForPolicy.tapePoolId   ),
      castor::dlf::Param("streamPolicyName"   , streamPolicyName             )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      FAILED_TO_FIND_TAPE_POOL_FOR_STREAM, params);

    return false;
  }

  TapePoolForStreamPolicy &tapePoolForPolicy = tapePoolItor->second;

  bool streamAcceptedByPolicy = false;
  {
    // Get a lock on the embedded Python-interpreter
    castor::tape::python::ScopedPythonLock scopedPythonLock;

    // Create the input-tuple for the stream-policy Python-function
    //
    // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
    // K must be used for unsigned (feature not documented at all but available)
    castor::tape::python::SmartPyObjectPtr inputObj(Py_BuildValue(
      (char *)"(K,K,K,K,K,s,K,K,s,K,K)",
      streamForPolicy.streamId,
      streamForPolicy.numTapeCopies,
      streamForPolicy.totalBytes,
      streamForPolicy.ageOfOldestTapeCopy,
      streamForPolicy.tapePoolId,
      tapePoolForPolicy.name.c_str(),
      tapePoolForPolicy.nbRunningStreams,
      svcClassId,
      svcClassName.c_str(),
      svcClassNbDrives,
      tapePoolsForPolicy.getTotalNbRunningStreams()));

    // Throw an exception if the creation of the input-tuple failed
    if(inputObj.get() == NULL) {
      // Try to determine the Python exception if there was a Python error
      PyObject *const pyEx = PyErr_Occurred();
      const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

      // Clear the Python error if there was one
      if(pyEx != NULL) {
        PyErr_Clear();
      }

      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Failed to create input-tuple for stream-policy Python-function"
        ": moduleName=stream"
        ", functionName=" << streamPolicyName <<
        ", pythonException=" << pyExStr;
      throw(ex);
    }

    // Call the stream-policy Python-function
    castor::tape::python::SmartPyObjectPtr resultObj(PyObject_CallObject(
      streamPolicyPyFunc, inputObj.get()));

    // Throw an exception if the stream-policy Python-function call failed
    if(resultObj.get() == NULL) {

      // Try to determine the Python exception if there was a Python error
      PyObject *const pyEx = PyErr_Occurred();
      const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

      // Clear the Python error if there was one
      if(pyEx != NULL) {
        PyErr_Clear();
      }

      castor::exception::Internal ex;

      ex.getMessage() <<
        "Failed to apply stream-policy to stream"
        ": Failed to execute stream-policy Python-function"
        ": streamId="                 << streamForPolicy.streamId            <<
        ", numTapeCopies="            << streamForPolicy.numTapeCopies       <<
        ", totalBytes="               << streamForPolicy.totalBytes          <<
        ", ageOfOldestTapeCopy="      << streamForPolicy.ageOfOldestTapeCopy <<
        ", tapePoolId="               << streamForPolicy.tapePoolId          <<
        ", tapePoolName="             << tapePoolForPolicy.name              <<
        ", tapePoolNbRunningStreams=" << tapePoolForPolicy.nbRunningStreams  <<
        ", svcClassId="               << svcClassId                          <<
        ", svcClassName="             << svcClassName                        <<
        ", svcClassNbDrives="         << svcClassNbDrives                    <<
        ", svcClassNbRunningStreams=" <<
          tapePoolsForPolicy.getTotalNbRunningStreams()                      <<
        ": streamPolicyName="         << streamPolicyName                    <<
        ": pythonException="          << pyExStr;

      throw ex;
    }

    streamAcceptedByPolicy = PyInt_AsLong(resultObj.get()) != 0;

  } // The lock on the embedded Python-interpreter is released here

  // Increment the number of running streams for the tape-pool if the
  // stream-policy has accepted the stream
  if(streamAcceptedByPolicy) {
    tapePoolForPolicy.nbRunningStreams++;
  }

  return streamAcceptedByPolicy;
}

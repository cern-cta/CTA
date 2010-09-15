/******************************************************************************
*                      RecHandlerDaemon.cpp
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
* @(#)$RCSfile: RecHandlerDaemon.cpp,v $ $Author: waldron $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

// Include Files

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include "castor/tape/python/python.hpp"

#include <Cgetopt.h>
#include <u64subr.h>
#include <string>
#include <memory>

#include "castor/Constants.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/server/SignalThreadPool.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"

#include "castor/tape/rechandler/Constants.hpp"
#include "castor/tape/rechandler/IRecHandlerSvc.hpp"
#include "castor/tape/rechandler/RecHandlerDaemon.hpp"
#include "castor/tape/rechandler/RecHandlerDlfMessageConstants.hpp"
#include "castor/tape/rechandler/RecHandlerThread.hpp"

// -----------------------------------------------------------------------
// External C function used for getting configuration from castor.conf file
// -----------------------------------------------------------------------


extern "C" {
  char* getconfent(const char *, const char *, int);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rechandler::RecHandlerDaemon::RecHandlerDaemon()
  throw() :
  castor::server::BaseDaemon("rechandlerd"),
  m_sleepTime(SLEEP_TIME){
}



//------------------------------------------------------------------------------
// main 
//------------------------------------------------------------------------------

int castor::tape::rechandler::RecHandlerDaemon::main(int argc, char* argv[]) {

  
  // Try to initialize the DLF logging system, quitting with an error message
  // to stderr if the initialization fails
  try {

    castor::server::BaseServer::dlfInit(s_dlfMessages);

  } catch(castor::exception::Exception &ex) {
    std::cerr << std::endl <<
      "Failed to start daemon"
      ": Failed to initialize DLF"
      ": " << ex.getMessage().str() << std::endl << std::endl;

    return 1;
  }

  // Try to start the daemon, quitting with an error message to stderr and DLF
  // if the start fails
  try {

    exceptionThrowingMain(argc, argv);

  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl << "Failed to start daemon: "
      << ex.getMessage().str() << std::endl << std::endl;

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
      RECHANDLER_FAILED_TO_START, params);

    return 1;
  }

  // Finalize Python

  castor::tape::python::finalizePython();

  return 0;
}


//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int castor::tape::rechandler::RecHandlerDaemon::exceptionThrowingMain(int argc,
  char **argv) throw(castor::exception::Exception) {

  // Log the start of the daemon
  logStart(argc, argv);

  // Check the service to access the database can be obtained
  
  castor::IService* orasvc = castor::BaseObject::services()->service("OraRecHandlerSvc", castor::SVC_ORARECHANDLERSVC);
  castor::tape::rechandler::IRecHandlerSvc* myService = dynamic_cast<castor::tape::rechandler::IRecHandlerSvc*>(orasvc);

  // Throw an exception if the Oracle database service could not
  // be obtained
  if (myService == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to get  OraRecHandler  database service";
    throw(ex);
  }


  //Retrive the retry policies

  std::string recallPolicyName;
  char* tmpStr=NULL;
  tmpStr = getconfent("RECHANDLER","RECALL_POLICY",0);
  if (tmpStr==NULL){
    castor::dlf::Param params[] =
      {castor::dlf::Param("message","No policy for migration retry in castor.conf")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::tape::rechandler::NO_POLICY, params);
  } else {
    recallPolicyName=tmpStr;
  }

  std::string recallFunctionName;

  tmpStr = getconfent("RECHANDLER","RECALL_POLICY_FUN",0);
  if (tmpStr == NULL ){
    castor::dlf::Param params[] =
      {castor::dlf::Param("message","No global function name for recall policy for recall in castor.conf")};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT, castor::tape::rechandler::NO_POLICY, params);
  } else {
    recallFunctionName=tmpStr;
  }

  castor::tape::python::initializePython();

  // Load the policies
  PyObject *recallDict = NULL;
  if (!recallPolicyName.empty()){
    recallDict =
      python::importPolicyPythonModuleWithLock(recallPolicyName.c_str());
  }
  
  // Get the rechandler-policy Python-function if the Python-module was present.
  // The name of the function is the same as the module.
  PyObject * recallFunction = NULL;
  if (recallDict && !recallFunctionName.empty()) {
    recallFunction = python::getPythonFunctionWithLock(recallDict,
      recallFunctionName.c_str());
  }

  // Check the arguments of the recall-policy Python-function are valid if the
  // function is present
  if(recallFunction != NULL) {
    // Load the Python inspect module
    PyObject *const inspectDict = python::importPythonModuleWithLock("inspect");

    // Get a handle on the inspect.getargspec Python-function
    PyObject *const inspectGetargspecFunc = python::getPythonFunctionWithLock(
      inspectDict, "getargspec");

    // Throw an exception if the handle on the Python-function could not be
    // obtained
    if(inspectGetargspecFunc == NULL) {
      // Try to determine the Python exception if there was a Python error
      PyObject *const pyEx = PyErr_Occurred();
      const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

      // Clear the Python error if there was one
      if(pyEx != NULL) {
        PyErr_Clear();
      }

      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Failed to get handle on Python-function"
        ": moduleName=inspect"
        ", functionName=getargspec"
        ", pythonException=" << pyExStr;
      throw(ex);
    }

    // Define the expected function-arguments
    std::vector<std::string> expectedRecallPolicyArgNames;
    expectedRecallPolicyArgNames.push_back("vid");
    expectedRecallPolicyArgNames.push_back("numSegments");
    expectedRecallPolicyArgNames.push_back("totalBytes");
    expectedRecallPolicyArgNames.push_back("ageOfOldestSegment");
    expectedRecallPolicyArgNames.push_back("priority");
    expectedRecallPolicyArgNames.push_back("nbMountsForRecall");

    // The following method will throw an InvalidConfiguration exception in
    // the case the expected and actual function-arguments do not match
    python::checkFuncArgNames(recallFunctionName.c_str(),
      expectedRecallPolicyArgNames, inspectGetargspecFunc, recallFunction);
  }

  parseCommandLine(argc,argv);

  // Force it to run as stage:st
  runAsStagerSuperuser();
  
  std::auto_ptr<RecHandlerThread>
    recThread(new RecHandlerThread(recallFunction));
  std::auto_ptr<castor::server::SignalThreadPool>
    recPool(new castor::server::SignalThreadPool("RecHandlerThread",
      recThread.release(), m_sleepTime));
  addThreadPool(recPool.release());
  getThreadPool('R')->setNbThreads(1);
  start();
  return 0;
}

//------------------------------------------------------------------------------
// logStart
//------------------------------------------------------------------------------

void castor::tape::rechandler::RecHandlerDaemon::logStart(const int argc,
  const char *const *const argv) throw() {
  std::string concatenatedArgs;

  // Concatenate all of the command-line arguments into one string
  for(int i=0; i < argc; i++) {
    if(i != 0) {
      concatenatedArgs += " ";
    }

    concatenatedArgs += argv[i];
  }

  castor::dlf::Param params[] = {
    castor::dlf::Param("argv", concatenatedArgs)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, DAEMON_START,params);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------

void castor::tape::rechandler::RecHandlerDaemon::parseCommandLine(int argc, char* argv[]){
  Coptind = 1;
  Copterr = 1;
  int c;
  while ( (c = Cgetopt(argc,argv,"t:fh")) != -1 ) {
    switch (c) {
    case 't':
      m_sleepTime = strutou64(Coptarg);
      break;
    case 'f':
      m_foreground = true;
      break;
    case 'h':
      usage();
      exit(0);
    default:
      usage();
      exit(0);
    }
  }
}

//------------------------------------------------------------------------------
// Usage
//------------------------------------------------------------------------------

void castor::tape::rechandler::RecHandlerDaemon::usage(){
  std::cout << "\nUsage: " << "RecHandlerDaemon"
            << " [options]\n "
            << "Where options are:\n"
            << "-f     : to run in foreground\n"
            << "-t sleepTime(seconds)  : sleep time (in seconds) between two checks. Default=300\n"
            << "-h : to ask for help\n"
	    <<std::endl;

}

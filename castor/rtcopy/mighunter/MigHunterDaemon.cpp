/******************************************************************************
*                      MigHunterDaemon.cpp
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
* @(#)$RCSfile: MigHunterDaemon.cpp,v $ $Author: waldron $
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
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/rtcopy/mighunter/Constants.hpp"
#include "castor/rtcopy/mighunter/IMigHunterSvc.hpp"
#include "castor/rtcopy/mighunter/MigHunterDaemon.hpp"
#include "castor/rtcopy/mighunter/MigHunterDlfMessageConstants.hpp"
#include "castor/rtcopy/mighunter/MigHunterThread.hpp"
#include "castor/rtcopy/mighunter/ScopedPythonLock.hpp"
#include "castor/rtcopy/mighunter/SmartFILEPtr.hpp"
#include "castor/rtcopy/mighunter/SmartPyObjectPtr.hpp"
#include "castor/rtcopy/mighunter/StreamThread.hpp"
#include "castor/server/SignalThreadPool.hpp"
#include "h/Cgetopt.h"
#include "h/u64subr.h"

#include <iostream>
#include <list>
#include <memory>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

extern "C" {
  char* getconfent(const char *, const char *, int);
}


//------------------------------------------------------------------------------
// s_instance
//------------------------------------------------------------------------------
castor::rtcopy::mighunter::MigHunterDaemon
  castor::rtcopy::mighunter::MigHunterDaemon::s_instance;


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::rtcopy::mighunter::MigHunterDaemon::MigHunterDaemon()
  throw() :
  castor::server::BaseDaemon("mighunterd"),
  m_streamSleepTime(STREAM_SLEEP_TIME),
  m_migrationSleepTime(MIGRATION_SLEEP_TIME),
  m_migrationDataThreshold(MIGRATION_DATA_THRESHOLD),
  m_doClone(false) {
}


//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::rtcopy::mighunter::MigHunterDaemon::main(const int argc,
  char **argv) {

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
    CASTOR_DLF_WRITEPC(nullCuuid, DLF_LVL_ERROR,
      MIGHUNTER_FAILED_TO_START, params);

    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int castor::rtcopy::mighunter::MigHunterDaemon::exceptionThrowingMain(int argc,
  char **argv) throw(castor::exception::Exception) {

  // Log the start of the daemon
  logStart(argc, argv);

  // Check the service to access the database can be obtained
  const char *orasvcName = "OraMigHunterSvc";
  castor::IService* orasvc = castor::BaseObject::services()->service(
    orasvcName, castor::SVC_ORAMIGHUNTERSVC);
  castor::rtcopy::mighunter::IMigHunterSvc* mySvc =
    dynamic_cast<castor::rtcopy::mighunter::IMigHunterSvc*>(orasvc);

  // Throw an exception if the Oracle database service could not
  // be obtained
  if (mySvc == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to get " << orasvcName << " Oracle database service";
    throw(ex);
  }

  // Append the CASTOR policies directory to the end of the PYTHONPATH
  // environment variable so the PyImport_ImportModule() function can find the
  // stream and migration policy modules
  appendDirectoryToPYTHONPATH(CASTOR_POLICIES_DIRECTORY);

  // Initialize Python
  Py_Initialize();

  // Initialize thread support
  //
  // Please note that PyEval_InitThreads() takes a lock on the global Python
  // interpreter
  PyEval_InitThreads();

  // Load in the stream policy Python module if one has been specified in
  // castor.conf and get a handle on its associated Python dictionary object
  SmartPyObjectPtr streamPolicyModule;
  PyObject *streamPolicyDict = NULL;
  {
    const char *const streamPolicyModuleName =
      getconfent("Policy", "Stream", 0);
    if(streamPolicyModuleName != NULL) {
      checkPolicyModuleFileExists(streamPolicyModuleName);
      streamPolicyModule.reset(importPythonModule(streamPolicyModuleName));
      streamPolicyDict = PyModule_GetDict(streamPolicyModule.get());
    }
  }

  // Load in the migration policy Python module if one has been specified in
  // castor.conf and get a handle on its associated Python dictionary object
  SmartPyObjectPtr migrationPolicyModule;
  PyObject *migrationPolicyDict = NULL;
  {
    const char *const migrationPolicyModuleName =
      getconfent("Policy", "Migration", 0);
    if(migrationPolicyModuleName != NULL) {
      checkPolicyModuleFileExists(migrationPolicyModuleName);
      migrationPolicyModule.reset(importPythonModule(
        migrationPolicyModuleName));
      migrationPolicyDict = PyModule_GetDict(migrationPolicyModule.get());
    }
  }

  // Parse the command-line
  parseCommandLine(argc, argv);

  // Clean up the mighunter data in the database
  for(std::list<std::string>::const_iterator svcClassName =
    m_listSvcClass.begin(); svcClassName != m_listSvcClass.end(); 
    svcClassName++) {
    mySvc->migHunterCleanUp(*svcClassName);
  }

  // Create the mighunter thread pool
  std::auto_ptr<castor::rtcopy::mighunter::MigHunterThread> migHunterThread(
    new MigHunterThread(m_listSvcClass, m_migrationDataThreshold, m_doClone,
    migrationPolicyDict));
  std::auto_ptr<castor::server::SignalThreadPool> migHunterPool(
    new server::SignalThreadPool("MigHunterThread", migHunterThread.release(),
    m_migrationSleepTime));
  addThreadPool(migHunterPool.release());
  getThreadPool('M')->setNbThreads(1);

  // Create the stream thread pool
  std::auto_ptr<StreamThread> streamThread(new StreamThread(m_listSvcClass,
    streamPolicyDict));
  std::auto_ptr<server::SignalThreadPool> streamPool(
    new server::SignalThreadPool("StreamThread", streamThread.release(),
    m_streamSleepTime));
  addThreadPool(streamPool.release());
  getThreadPool('S')->setNbThreads(1);

  // start the threads
  start();

  return 0;
}


//------------------------------------------------------------------------------
// logStart
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterDaemon::logStart(const int argc,
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
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, DAEMON_START, params);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterDaemon::parseCommandLine(int argc,
  char* argv[]){

  if (argc < 1 ) {
    usage();
    return;
  }
  Coptind = 1;
  Copterr = 1;
  int c; // ????

  std::string optionStr="Option used: ";

  while ( (c = Cgetopt(argc,argv,"Ct:u:v:fh")) != -1 ) {
    switch (c) {
    case 'C':
      optionStr+=" -C ";
      m_doClone = true;
      break;
    case 't':
      optionStr+=" -t ";
      optionStr+=Coptarg;
      m_streamSleepTime = strutou64(Coptarg);
      break;
    case 'u':
      optionStr+=" -u ";
      optionStr+=Coptarg;
      m_migrationSleepTime = strutou64(Coptarg);
    case 'v':
      optionStr+=" -v ";
      optionStr+=Coptarg;
      m_migrationDataThreshold = strutou64(Coptarg);
      break;
    case 'f':
      optionStr+=" -f ";
      m_foreground = true;
      break;
    case 'h':
      optionStr+=" -h ";
      usage();
      exit(0);
    default:
      usage();
      exit(0);
    }
  }

  std::string svcClassesStr="Used the following Svc Classes: ";

  for (int i=Coptind; i<argc; i++ ) {
   m_listSvcClass.push_back(argv[i]);
   svcClassesStr+=" ";
   svcClassesStr+=argv[i];
   svcClassesStr+=" ";
  }
  if (m_listSvcClass.empty()) {
    m_listSvcClass.push_back("default");
    svcClassesStr+=" default ";
  }

  castor::dlf::Param params[] = {
    castor::dlf::Param("option"    , optionStr.c_str()    ),
    castor::dlf::Param("svcClasses", svcClassesStr.c_str())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, PARSING_OPTIONS, 2,
    params);
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterDaemon::usage(){
  std::cout <<
    "\nUsage: MigHunterDaemon [options] svcClass1 svcClass2 svcClass3 ...\n"
    "\n"
    "-C          : clone tapecopies from existing to new streams (very slow!)\n"
    "-f          : to run in foreground\n"
    "-t sleepTime: sleep time (in seconds) between two checks. Default= 7200\n"
    "-v volume   : data volume threshold in bytes below which a migration\n"
    "              will not start\n" << std::endl;
}


//------------------------------------------------------------------------------
// appendDirectoryToPYTHONPATH
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterDaemon::appendDirectoryToPYTHONPATH(
  const char *const directory) throw() {
  // Get the current value of PYTHONPATH (there may not be one)
  const char *const currentPath = getenv("PYTHONPATH");

  // Construct the new value of PYTHONPATH
  std::string newPath;
  if(currentPath == NULL) {
    newPath = directory;
  } else {
    newPath  = currentPath;
    newPath += ":";
    newPath += directory;
  }

  // Set PYTHONPATH to the new value
  const int overwrite = 1;
  setenv("PYTHONPATH", newPath.c_str(), overwrite);
}


//------------------------------------------------------------------------------
// importPythonModule
//------------------------------------------------------------------------------
PyObject *castor::rtcopy::mighunter::MigHunterDaemon::importPythonModule(
  const char *const moduleName) throw(castor::exception::Exception) {

  PyObject *const module = PyImport_ImportModule((char *)moduleName);

  if(module == NULL) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyImport_ImportModule() call failed"
      ": moduleName=" << moduleName;

    throw(ex);
  }

  return module;
}


//------------------------------------------------------------------------------
// checkPolicyModuleFileExists
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::MigHunterDaemon::checkPolicyModuleFileExists(
  const char *moduleName) throw(castor::exception::Exception) {

  std::string fullPathname(CASTOR_POLICIES_DIRECTORY);

  fullPathname += "/";
  fullPathname += moduleName;
  fullPathname += ".py";

  struct stat buf;
  const int rc = stat(fullPathname.c_str(), &buf);
  const int savedErrno = errno;

  // Throw an exception if the stat() call failed
  if(rc != 0) {
    castor::exception::Exception ex(savedErrno);

    ex.getMessage() <<
      "stat() call failed"
      ": filename=" << fullPathname <<
      ": " << sstrerror(savedErrno);

    throw(ex);
  }

  // Throw an exception if the file is not a regular file
  if(!S_ISREG(buf.st_mode)) {
    castor::exception::Exception ex(savedErrno);

    ex.getMessage() <<
      fullPathname << " is not a regular file";

    throw(ex);
  }
}

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
#include "castor/tape/python/python.hpp"

#include <iostream>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "castor/Constants.hpp"
#include "castor/Services.hpp"

#include "castor/dlf/Dlf.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include "h/Cgetopt.h"
#include "h/u64subr.h"

#include "castor/server/SignalThreadPool.hpp"

#include "castor/tape/mighunter/Constants.hpp"
#include "castor/tape/mighunter/IMigHunterSvc.hpp"
#include "castor/tape/mighunter/MigHunterDaemon.hpp"
#include "castor/tape/mighunter/MigHunterDlfMessageConstants.hpp"
#include "castor/tape/mighunter/MigHunterThread.hpp"
#include "castor/tape/mighunter/StreamThread.hpp"

#include "castor/tape/utils/SmartFILEPtr.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"


extern "C" {
  char* getconfent(const char *, const char *, int);
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mighunter::MigHunterDaemon::MigHunterDaemon()
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
int castor::tape::mighunter::MigHunterDaemon::main(const int argc,
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
int castor::tape::mighunter::MigHunterDaemon::exceptionThrowingMain(int argc,
  char **argv) throw(castor::exception::Exception) {

  // Log the start of the daemon
  logStart(argc, argv);

  // Check the service to access the database can be obtained
  const char *orasvcName = "OraMigHunterSvc";
  castor::IService* orasvc = castor::BaseObject::services()->service(
    orasvcName, castor::SVC_ORAMIGHUNTERSVC);
  castor::tape::mighunter::IMigHunterSvc* mySvc =
    dynamic_cast<castor::tape::mighunter::IMigHunterSvc*>(orasvc);

  // Throw an exception if the Oracle database service could not
  // be obtained
  if (mySvc == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() <<
      "Failed to get " << orasvcName << " Oracle database service";
    throw(ex);
  }

  // The status of the migration-policy Python-module as a text message which
  // can be logged so that operators know what needs to be done in order to get
  // a user-defined policy loaded.  The possible string values are
  // "Not configured", "Not loaded" and "Loaded".
  const char *migrationPolicyModuleStatus = "Not configured";

  // Get the policy migration policy name
  std::string migrationPolicyModuleName;
  {
    const char *const category = "MIGHUNTER";
    const char *const name     = "MIGRATION_POLICY";
    const char *const tmpStr   = getconfent(category, name, 0);

    if(tmpStr != NULL) {
      migrationPolicyModuleName   = tmpStr;
      migrationPolicyModuleStatus = "Not loaded";
    } else {
      std::ostringstream oss;

      oss << category << "/" << name << " not specified in castor.conf";

      castor::dlf::Param params[] = {
        castor::dlf::Param("message", oss.str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
        MIGRATION_POLICY_NOT_CONFIGURED, params);
    }
  }

  // Get the policy stream policy name
  std::string streamPolicyModuleName;
  {
    const char *const category = "MIGHUNTER";
    const char *const name     = "STREAM_POLICY";
    const char *const tmpStr   = getconfent(category, name, 0);
 
    if(tmpStr != NULL) {
      streamPolicyModuleName= tmpStr;
    } else {
      std::ostringstream oss;

      oss << category << "/" << name << " not specified in castor.conf";

      castor::dlf::Param params[] = {
        castor::dlf::Param("message", oss.str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
        STREAM_POLICY_NOT_CONFIGURED, params);
    }
  }

  // Initialize Python

  castor::tape::python::initializePython();

  // Load the policies

  castor::tape::python::ScopedPythonLock scopedLock;

  PyObject* migrationPolicyDict = NULL;

  // Get the dictionary just if there is a module name set in castor.conf
  if (!migrationPolicyModuleName.empty()){
    migrationPolicyDict = castor::tape::python::importPolicyPythonModule(
      migrationPolicyModuleName.c_str());
  }

  // Update the status string of the migration-policy Python-module
  if(migrationPolicyDict != NULL) {
    migrationPolicyModuleStatus = "Loaded";
  }

  PyObject* streamPolicyDict = NULL;

  // Get the dictionary just if there is a module name set in castor.conf
  if (!streamPolicyModuleName.empty()){
    streamPolicyDict = castor::tape::python::importPolicyPythonModule(
      streamPolicyModuleName.c_str());
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
  std::auto_ptr<castor::tape::mighunter::MigHunterThread> migHunterThread(
    new MigHunterThread(m_listSvcClass, m_migrationDataThreshold, m_doClone,
    migrationPolicyDict, migrationPolicyModuleStatus));
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

  // Finalize Python
  castor::tape::python::finalizePython();
  
  return 0;
}


//------------------------------------------------------------------------------
// logStart
//------------------------------------------------------------------------------
void castor::tape::mighunter::MigHunterDaemon::logStart(const int argc,
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
void castor::tape::mighunter::MigHunterDaemon::parseCommandLine(int argc,
  char* argv[]){

  if (argc < 1 ) {
    usage();
    return;
  }

  std::string optionStr="Option used: ";

  // Booleans used to enforce the rule that the -t option must not be used in
  // conjunction with the -s or -m options.
  bool tOptionSet = false;
  bool sOptionSet = false;
  bool mOptionSet = false;

  Coptind = 1;
  Copterr = 1;
  int c = 0;
  while ( (c = Cgetopt(argc,argv,"fCt:s:m:v:h")) != -1 ) {
    switch (c) {
    case 'f':
      optionStr+=" -f ";
      m_foreground = true;
      break;
    case 'C':
      optionStr+=" -C ";
      m_doClone = true;
      break;
    case 't':
      tOptionSet = true;
      optionStr+=" -t ";
      optionStr+=Coptarg;
      m_streamSleepTime    = strutou64(Coptarg);
      m_migrationSleepTime = m_streamSleepTime;
      break;
    case 's':
      sOptionSet = true;
      optionStr+=" -s ";
      optionStr+=Coptarg;
      m_streamSleepTime = strutou64(Coptarg);
      break;
    case 'm':
      mOptionSet = true;
      optionStr+=" -m ";
      optionStr+=Coptarg;
      m_migrationSleepTime = strutou64(Coptarg);
    case 'v':
      optionStr+=" -v ";
      optionStr+=Coptarg;
      m_migrationDataThreshold = strutou64(Coptarg);
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

  // Enforce the rule that the -t option must not be used in
  // conjunction with the -s or -m options.
  if(tOptionSet && (sOptionSet || mOptionSet)) {
    std::cerr <<
      "The -t option cannot been specified in conjunction with the -s or -m\n"
      "options\n" << std::endl;

    usage();
    exit(0);
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
void castor::tape::mighunter::MigHunterDaemon::usage(){
  std::cout <<
    "\nUsage: MigHunterDaemon [options] svcClass1 svcClass2 svcClass3 ...\n"
    "\n"
    "-f       : Run the daemon in the foreground\n"
    "-C       : Clone tapecopies from existing to new streams (very slow!)\n"
    "-t time  : Single value in seconds to be used for both the sleep time\n"
    "           between two stream-policy database lookups and the sleep\n"
    "           time between two migration-policy database lookups. If this\n"
    "           option is not specified then the default value of 7200\n"
    "           seconds will be used\n"
    "-s time  : Sleep time in seconds between two stream-policy database\n"
    "           lookups. If this option is not specified then the default\n"
    "           value of 7200 seconds will be used\n"
    "-m time  : Sleep time in seconds between two migration-policy database\n"
    "           lookups. If this option is not specified then the default\n"
    "           value of 7200 seconds will be used\n"
    "-v volume: Data volume threshold in bytes below which a migration will\n"
    "           not start\n" << std::endl;
}


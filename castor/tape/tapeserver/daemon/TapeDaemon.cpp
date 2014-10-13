/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
 
#include "castor/common/CastorConfiguration.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/BadAlloc.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/CleanerSession.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/LabelCmdAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForker.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerConnectionHandler.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxySocket.hpp"
#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"
#include "castor/tape/tapeserver/daemon/TapeMessageHandler.hpp"
#include "castor/tape/tapeserver/daemon/VdqmAcceptHandler.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape.h"
#include "h/rmc_constants.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_constants.h"

#include <algorithm>
#include <errno.h>
#include <limits.h>
#include <memory>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::TapeDaemon(
  const int argc,
  char **const argv,
  std::ostream &stdOut,
  std::ostream &stdErr,
  log::Logger &log,
  const int netTimeout,
  const utils::DriveConfigMap &driveConfigs,
  legacymsg::CupvProxy &cupv,
  legacymsg::VdqmProxy &vdqm,
  legacymsg::VmgrProxy &vmgr,
  reactor::ZMQReactor &reactor,
  castor::server::ProcessCap &capUtils):
  castor::server::Daemon(stdOut, stdErr, log),
  m_argc(argc),
  m_argv(argv),
  m_netTimeout(netTimeout),
  m_driveConfigs(driveConfigs),
  m_cupv(cupv),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_reactor(reactor),
  m_capUtils(capUtils),
  m_programName("tapeserverd"),
  m_hostName(getHostName()),
  m_processForker(NULL),
  m_processForkerPid(0),
  m_catalogue(NULL),
  m_zmqContext(NULL) {
}

//------------------------------------------------------------------------------
// getHostName
//------------------------------------------------------------------------------
std::string
  castor::tape::tapeserver::daemon::TapeDaemon::getHostName()
  const  {
  char nameBuf[81];
  if(gethostname(nameBuf, sizeof(nameBuf))) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get host name: " << message;
    throw ex;
  }

  return nameBuf;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::~TapeDaemon() throw() {
  if(NULL != m_processForker) {
    m_processForker->stopProcessForker("TapeDaemon destructor called");
    delete m_processForker;
  }
  delete m_catalogue;
  destroyZmqContext();
  google::protobuf::ShutdownProtobufLibrary();
}

//------------------------------------------------------------------------------
// destroyZmqContext
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::destroyZmqContext() throw() {
  if(NULL != m_zmqContext) {
    if(zmq_term(m_zmqContext)) {
      char message[100];
      sstrerror_r(errno, message, sizeof(message));
      castor::log::Param params[] = {castor::log::Param("message", message)};
      m_log(LOG_ERR, "Failed to destroy ZMQ context", params);
    } else {
      m_zmqContext = NULL;
      m_log(LOG_INFO, "Successfully destroyed ZMQ context");
    }
  }
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::TapeDaemon::main() throw() {
  try {

    exceptionThrowingMain(m_argc, m_argv);

  } catch (castor::exception::Exception &ex) {
    // Write the error to standard error
    m_stdErr << std::endl << "Aborting: " << ex.getMessage().str() << std::endl
      << std::endl;

    // Log the error
    log::Param params[] = {
      log::Param("Message", ex.getMessage().str()),
      log::Param("Code"   , ex.code())};
    m_log(LOG_ERR, "Aborting", params);

    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
void  castor::tape::tapeserver::daemon::TapeDaemon::exceptionThrowingMain(
  const int argc, char **const argv)  {
  logStartOfDaemon(argc, argv);
  parseCommandLine(argc, argv);

  // Process must be able to change user now and should be permitted to perform
  // raw IO in the future
  setProcessCapabilities("cap_setgid,cap_setuid+ep cap_sys_rawio+p");

  const bool runAsStagerSuperuser = true;
  daemonizeIfNotRunInForeground(runAsStagerSuperuser);
  setDumpable();

  // Create two socket pairs for ProcessForker communications
  const ForkerCmdPair cmdPair = createForkerCmdPair();
  const ForkerReaperPair reaperPair = createForkerReaperPair();

  m_processForkerPid = forkProcessForker(cmdPair, reaperPair);

  const DataTransferSession::CastorConf dataTransferConfig =
    getDataTransferConf();
  m_processForker = new ProcessForkerProxySocket(m_log, cmdPair.tapeDaemon);
  m_catalogue = new Catalogue(m_netTimeout, m_log, dataTransferConfig,
    *m_processForker, m_cupv, m_vdqm, m_vmgr, m_hostName);

  m_catalogue->populate(m_driveConfigs);

  // There is no longer any need for the process to be able to change user,
  // however the process should still be permitted to perform raw IO in the
  // future
  setProcessCapabilities("cap_sys_rawio+p");

  blockSignals();
  initZmqContext();
  setUpReactor(reaperPair.tapeDaemon);
  registerTapeDrivesWithVdqm();
  mainEventLoop();
}

//------------------------------------------------------------------------------
// logStartOfDaemon
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logStartOfDaemon(
  const int argc, const char *const *const argv) throw() {
  const std::string concatenatedArgs = argvToString(argc, argv);
  std::ostringstream msg;
  msg << m_programName << " started";

  log::Param params[] = {
    log::Param("argv", concatenatedArgs)};
  m_log(LOG_INFO, msg.str(), params);
}

//------------------------------------------------------------------------------
// argvToString
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::TapeDaemon::argvToString(
  const int argc, const char *const *const argv) throw() {
  std::string str;

  for(int i=0; i < argc; i++) {
    if(i != 0) {
      str += " ";
    }

    str += argv[i];
  }
  return str;
}

//------------------------------------------------------------------------------
// setDumpable
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setDumpable() {
  castor::utils::setDumpableProcessAttribute(true);
  const bool dumpable = castor::utils::getDumpableProcessAttribute();
  log::Param params[] = {
    log::Param("dumpable", dumpable ? "true" : "false")};
  m_log(LOG_INFO, "Got dumpable attribute of process", params);
  if(!dumpable) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set dumpable attribute of process to true";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setProcessCapabilities(
  const std::string &text) {
  try {
    m_capUtils.setProcText(text);
    log::Param params[] =
      {log::Param("capabilities", m_capUtils.getProcText())};
    m_log(LOG_INFO, "Set process capabilities", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set process capabilities to '" << text <<
      "': " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// forkProcessForker
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::TapeDaemon::forkProcessForker(
  const ForkerCmdPair &cmdPair, const ForkerReaperPair &reaperPair) {
  m_log.prepareForFork();

  const pid_t forkRc = fork();

  // If fork failed
  if(0 > forkRc) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));

    closeForkerCmdPair(cmdPair);
    closeForkerReaperPair(reaperPair);

    castor::exception::Exception ex;
    ex.getMessage() << "Failed to fork ProcessForker: " << message;
    throw ex;

  // Else if this is the parent process
  } else if(0 < forkRc) {
    {
      log::Param params[] = {
        log::Param("processForkerPid", forkRc)};
      m_log(LOG_INFO, "Successfully forked the ProcessForker", params);
    }

    closeProcessForkerSideOfCmdPair(cmdPair);
    closeProcessForkerSideOfReaperPair(reaperPair);

    return forkRc;

  // Else this is the child process
  } else {
    closeTapeDaemonSideOfCmdPair(cmdPair);
    closeTapeDaemonSideOfReaperPair(reaperPair);

    castor::utils::setProcessNameAndCmdLine(m_argv[0], "tpforker");

    exit(runProcessForker(cmdPair.processForker, reaperPair.processForker));
  }
}

//------------------------------------------------------------------------------
// createForkerCmdPair
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::ForkerCmdPair
  castor::tape::tapeserver::daemon::TapeDaemon::createForkerCmdPair() {
  ForkerCmdPair cmdPair;

  try {
    const std::pair<int, int> socketPair = createSocketPair();
    cmdPair.tapeDaemon = socketPair.first;
    cmdPair.processForker = socketPair.second;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create socket pair to control the"
      " ProcessForker: " << ne.getMessage().str();
    throw ex; 
  }

  {
    log::Param params[] = {
      log::Param("cmdPair.tapeDaemon", cmdPair.tapeDaemon),
      log::Param("cmdPair.processForker", cmdPair.processForker)};
    m_log(LOG_INFO, "TapeDaemon parent process succesfully created socket"
      " pair to control the ProcessForker", params);
  }

  return cmdPair;
}

//------------------------------------------------------------------------------
// createForkerReaperPair
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TapeDaemon::ForkerReaperPair
  castor::tape::tapeserver::daemon::TapeDaemon::createForkerReaperPair() {
  ForkerReaperPair reaperPair;

  try {
    const std::pair<int, int> socketPair = createSocketPair();
    reaperPair.tapeDaemon = socketPair.first;
    reaperPair.processForker = socketPair.second;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create socket pair for the ProcessForker"
      " to report terminated processes: " << ne.getMessage().str();
    throw ex;
  }

  {
    log::Param params[] = {
      log::Param("reaperPair.tapeDaemon", reaperPair.tapeDaemon),
      log::Param("reaperPair.processForker", reaperPair.processForker)};
    m_log(LOG_INFO, "TapeDaemon parent process succesfully created socket"
      " pair for ProcessForker to report terminated processes", params);
  }

  return reaperPair;
}

//------------------------------------------------------------------------------
// createSocketPair
//------------------------------------------------------------------------------
std::pair<int, int>
  castor::tape::tapeserver::daemon::TapeDaemon::createSocketPair() {
  int sv[2] = {-1, -1};
  if(socketpair(AF_LOCAL, SOCK_STREAM, 0, sv)) {
    char message[100];
    strerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create socket pair: " << message;
    throw ex;
  }

  return std::pair<int, int> (sv[0], sv[1]);
}

//------------------------------------------------------------------------------
// closeForkerCmdPair
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::closeForkerCmdPair(
  const ForkerCmdPair &cmdPair) const {
  if(close(cmdPair.tapeDaemon)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to close TapeDaemon side of cmdPair"
      ": cmdPair.tapeDaemon=" << cmdPair.tapeDaemon << ": " << message;
    throw ex;
  }

  if(close(cmdPair.processForker)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to close ProcessForker side of cmdPair"
      ": cmdPair.processForker=" << cmdPair.processForker << ": " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// closeForkerReaperPair
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::closeForkerReaperPair(
  const ForkerReaperPair &reaperPair) const {
  if(close(reaperPair.tapeDaemon)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to close TapeDaemon side of reaperPair"
      ": reaperPair.tapeDaemon=" << reaperPair.tapeDaemon << ": " << message;
    throw ex;
  }

  if(close(reaperPair.processForker)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to close ProcessForker side of reaperPair"
      ": reaperPair.processForker=" << reaperPair.processForker << ": " <<
      message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// closeProcessForkerSideOfCmdPair
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  closeProcessForkerSideOfCmdPair(const ForkerCmdPair &cmdPair) const {
  if(close(cmdPair.processForker)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "TapeDaemon parent process failed to close"
      " ProcessForker side of cmdPair: cmdPair.processForker=" <<
      cmdPair.processForker << ": " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// closeProcessForkerSideOfReaperPair
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  closeProcessForkerSideOfReaperPair(const ForkerReaperPair &reaperPair)
  const {
  if(close(reaperPair.processForker)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "TapeDaemon parent process failed to close"
      " ProcessForker side of reaperPair: reaperPair.processForker=" <<
      reaperPair.processForker << ": " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// closeTapeDaemonSideOfCmdPair
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  closeTapeDaemonSideOfCmdPair(const ForkerCmdPair &cmdPair) const {
  if(close(cmdPair.tapeDaemon)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "ProcessForker process failed to close"
      " TapeDaemon side of cmdPair: cmdPair.tapeDaemon=" << cmdPair.tapeDaemon
      << ": " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// closeTapeDaemonSideOfReaperPair
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  closeTapeDaemonSideOfReaperPair(const ForkerReaperPair &reaperPair) const {
  if(close(reaperPair.tapeDaemon)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "ProcessForker process failed to close"
      " TapeDaemon side of reaperPair: reaperPair.tapeDaemon=" <<
      reaperPair.tapeDaemon << ": " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// runProcessForker
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::TapeDaemon::runProcessForker(
  const int cmdReceiverSocket, const int reaperSenderSocket) throw() {
  try {
    ProcessForker processForker(m_log, cmdReceiverSocket, reaperSenderSocket,
      m_hostName, m_argv[0]);
    processForker.execute();
    return 0;
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "ProcessForker threw an unexpected exception", params);
  } catch(std::exception &se) {
    log::Param params[] = {log::Param("message", se.what())};
    m_log(LOG_ERR, "ProcessForker threw an unexpected exception", params);
  } catch(...) {
    m_log(LOG_ERR, "ProcessForker threw an unknown and unexpected exception");
  }
  return 1;
}

//------------------------------------------------------------------------------
// blockSignals
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::blockSignals() const {
  sigset_t sigs;
  sigemptyset(&sigs);
  // The signals that should not asynchronously disturb the daemon
  sigaddset(&sigs, SIGHUP);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGQUIT);
  sigaddset(&sigs, SIGPIPE);
  sigaddset(&sigs, SIGTERM);
  sigaddset(&sigs, SIGUSR1);
  sigaddset(&sigs, SIGUSR2);
  sigaddset(&sigs, SIGCHLD);
  sigaddset(&sigs, SIGTSTP);
  sigaddset(&sigs, SIGTTIN);
  sigaddset(&sigs, SIGTTOU);
  sigaddset(&sigs, SIGPOLL);
  sigaddset(&sigs, SIGURG);
  sigaddset(&sigs, SIGVTALRM);
  castor::exception::Errnum::throwOnNonZero(
    sigprocmask(SIG_BLOCK, &sigs, NULL),
    "Failed to block signals: sigprocmask() failed");
}

//------------------------------------------------------------------------------
// registerTapeDrivesWithVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::registerTapeDrivesWithVdqm()
  {
  const std::list<std::string> unitNames = m_catalogue->getUnitNames();

  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor != unitNames.end(); itor++) {
    registerTapeDriveWithVdqm(*itor);
  }
}

//------------------------------------------------------------------------------
// registerTapeDriveWithVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::registerTapeDriveWithVdqm(
  const std::string &unitName)  {
  const CatalogueDrive &drive = m_catalogue->findDrive(unitName);
  const utils::DriveConfig &driveConfig = drive.getConfig();

  std::list<log::Param> params;
  params.push_back(log::Param("server", m_hostName));
  params.push_back(log::Param("unitName", unitName));
  params.push_back(log::Param("dgn", driveConfig.dgn));

  switch(drive.getState()) {
  case CatalogueDrive::DRIVE_STATE_DOWN:
    params.push_back(log::Param("state", "down"));
    m_log(LOG_INFO, "Registering tape drive in vdqm", params);
    m_vdqm.setDriveDown(m_hostName, unitName, driveConfig.dgn);
    break;
  case CatalogueDrive::DRIVE_STATE_UP:
    params.push_back(log::Param("state", "up"));
    m_log(LOG_INFO, "Registering tape drive in vdqm", params);
    m_vdqm.setDriveUp(m_hostName, unitName, driveConfig.dgn);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to register tape drive in vdqm"
        ": server=" << m_hostName << " unitName=" << unitName << " dgn=" <<
        driveConfig.dgn << ": Invalid drive state: state=" <<
        CatalogueDrive::drvState2Str(drive.getState());
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// initZmqContext
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::initZmqContext() {
  const int sizeOfIOThreadPoolForZMQ = 1;
  m_zmqContext = zmq_init(sizeOfIOThreadPoolForZMQ);
  if(NULL == m_zmqContext) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate ZMQ context: " << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setUpReactor
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setUpReactor(
  const int reaperSocket) {
  createAndRegisterProcessForkerConnectionHandler(reaperSocket);
  createAndRegisterVdqmAcceptHandler();
  createAndRegisterAdminAcceptHandler();
  createAndRegisterLabelCmdAcceptHandler();
  createAndRegisterTapeMessageHandler();
}

//------------------------------------------------------------------------------
// createAndRegisterProcessForkerConnectionHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  createAndRegisterProcessForkerConnectionHandler(const int reaperSocket)  {
  try {
    std::auto_ptr<ProcessForkerConnectionHandler> handler;
    try {
      handler.reset(new ProcessForkerConnectionHandler(reaperSocket, m_reactor,
        m_log, *m_catalogue));
    } catch(std::bad_alloc &ba) {
      castor::exception::BadAlloc ex;
      ex.getMessage() <<
        "Failed to create event handler for communicating with the ProcessForker"
        ": " << ba.what();
      throw ex;
    }
    m_reactor.registerHandler(handler.get());
    handler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create and register ProcessForkerConnectionHandler: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAndRegisterVdqmAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  createAndRegisterVdqmAcceptHandler()  {
  try {
    castor::utils::SmartFd listenSock;
    try {
      listenSock.reset(io::createListenerSock(TAPESERVER_VDQM_LISTENING_PORT));
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex(ne.code());
      ex.getMessage() << "Failed to create socket to listen for vdqm connections"
        ": " << ne.getMessage().str();
      throw ex;
    }
    {
      log::Param params[] = {
        log::Param("listeningPort", TAPESERVER_VDQM_LISTENING_PORT)};
      m_log(LOG_INFO, "Listening for connections from the vdqmd daemon", params);
    }

    std::auto_ptr<VdqmAcceptHandler> handler;
    try {
      handler.reset(new VdqmAcceptHandler(listenSock.get(), m_reactor, m_log,
        *m_catalogue));
      listenSock.release();
    } catch(std::bad_alloc &ba) {
      castor::exception::BadAlloc ex;
      ex.getMessage() <<
        "Failed to create event handler for accepting vdqm connections"
        ": " << ba.what();
      throw ex;
    }
    m_reactor.registerHandler(handler.get());
    handler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create and register VdqmAcceptHandler: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAndRegisterAdminAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  createAndRegisterAdminAcceptHandler()  {
  try {
    castor::utils::SmartFd listenSock;
    try {
      listenSock.reset(io::createListenerSock(TAPESERVER_ADMIN_LISTENING_PORT));
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex(ne.code());
      ex.getMessage() <<
        "Failed to create socket to listen for admin command connections"
        ": " << ne.getMessage().str();
      throw ex;
    }
    {
      log::Param params[] = {
        log::Param("listeningPort", TAPESERVER_ADMIN_LISTENING_PORT)};
      m_log(LOG_INFO, "Listening for connections from the admin commands",
        params);
    }

    std::auto_ptr<AdminAcceptHandler> handler;
    try {
      handler.reset(new AdminAcceptHandler(listenSock.get(), m_reactor, m_log,
        m_vdqm, *m_catalogue, m_hostName));
      listenSock.release();
    } catch(std::bad_alloc &ba) {
      castor::exception::BadAlloc ex;
      ex.getMessage() <<
        "Failed to create event handler for accepting admin connections"
        ": " << ba.what();
      throw ex;
    }
    m_reactor.registerHandler(handler.get());
    handler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create and register AdminAcceptHandler: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAndRegisterLabelCmdAcceptHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  createAndRegisterLabelCmdAcceptHandler()  {
  try {
    castor::utils::SmartFd listenSock;
    try {
      listenSock.reset(
        io::createListenerSock(TAPESERVER_LABELCMD_LISTENING_PORT));
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex(ne.code());
      ex.getMessage() <<
        "Failed to create socket to listen for admin command connections"
        ": " << ne.getMessage().str();
      throw ex;
    }
    {
      log::Param params[] = {
        log::Param("listeningPort", TAPESERVER_LABELCMD_LISTENING_PORT)};
      m_log(LOG_INFO, "Listening for connections from label command",
        params);
    }

    std::auto_ptr<LabelCmdAcceptHandler> handler;
    try {
      handler.reset(new LabelCmdAcceptHandler(listenSock.get(), m_reactor, m_log,
        *m_catalogue, m_hostName, m_vdqm, m_vmgr));
      listenSock.release();
    } catch(std::bad_alloc &ba) {
      castor::exception::BadAlloc ex;
      ex.getMessage() <<
        "Failed to create event handler for accepting label-command connections"
        ": " << ba.what();
      throw ex;
    }
    m_reactor.registerHandler(handler.get());
    handler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create and register LabelCmdAcceptHandler: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createAndRegisterTapeMessageHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::
  createAndRegisterTapeMessageHandler()  {
  try {
    std::auto_ptr<TapeMessageHandler> handler;
    try {
      handler.reset(new TapeMessageHandler(m_reactor, m_log, *m_catalogue,
        m_hostName, m_vdqm, m_vmgr, m_zmqContext));
    } catch(std::bad_alloc &ba) {
      castor::exception::BadAlloc ex;
      ex.getMessage() <<
        "Failed to create event handler for communicating with forked sessions"
        ": " << ba.what();
      throw ex;
    }
    m_reactor.registerHandler(handler.get());
    handler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to create and register TapeMessageHandler: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// mainEventLoop
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::mainEventLoop() {
  while(handleEvents()) {
  }
}

//------------------------------------------------------------------------------
// handleEvents
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeDaemon::handleEvents()
   {
  // With our current understanding we see no reason for an exception from the
  // reactor to be used as a reason to stop the tapeserverd daemon.
  //
  // In the future, one or more specific exception types could be introduced
  // with their own catch clauses that do in fact require the tapeserverd daemon
  // to be stopped.
  try {
    const int timeout = 100; // 100 milliseconds
    m_reactor.handleEvents(timeout);
  } catch(castor::exception::Exception &ex) {
    // Log exception and continue
    log::Param params[] = {
      log::Param("message", ex.getMessage().str()),
      log::Param("backtrace", ex.backtrace())
    };
    m_log(LOG_ERR,
      "Unexpected castor exception thrown when handling an I/O event", params);
  } catch(std::exception &se) {
    // Log exception and continue
    log::Param params[] = {log::Param("message", se.what())};
    m_log(LOG_ERR, "Unexpected exception thrown when handling an I/O event",
      params);
  } catch(...) {
    // Log exception and continue
    m_log(LOG_ERR,
      "Unexpected and unknown exception thrown when handling an I/O event");
  }

  m_catalogue->tick();

  return handlePendingSignals();
}

//------------------------------------------------------------------------------
// handlePendingSignals
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::TapeDaemon::handlePendingSignals()
  throw() {
  bool continueMainEventLoop = true;
  int sig = 0;
  sigset_t allSignals;
  siginfo_t sigInfo;
  sigfillset(&allSignals);
  struct timespec immediateTimeout = {0, 0};

  // While there is a pending signal to be handled
  while (0 < (sig = sigtimedwait(&allSignals, &sigInfo, &immediateTimeout))) {
    switch(sig) {
    case SIGINT: // Signal number 2
      m_log(LOG_INFO, "Stopping gracefully because SIGINT was received");
      continueMainEventLoop = false;
      break;
    case SIGTERM: // Signal number 15
      m_log(LOG_INFO, "Stopping gracefully because SIGTERM was received");
      continueMainEventLoop = false;
      break;
    case SIGCHLD: // Signal number 17
      reapZombies();
      break;
    default:
      {
        log::Param params[] = {log::Param("signal", sig)};
        m_log(LOG_INFO, "Ignoring signal", params);
      }
      break;
    }
  }

  return continueMainEventLoop;
}

//------------------------------------------------------------------------------
// reapZombies
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::reapZombies() throw() {
  pid_t pid = 0;
  int waitpidStat = 0;
  while (0 < (pid = waitpid(-1, &waitpidStat, WNOHANG))) {
    handleReapedProcess(pid, waitpidStat);
  }
}

//------------------------------------------------------------------------------
// handleReapedProcess
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedProcess(
  const pid_t pid, const int waitpidStat) throw() {
  logChildProcessTerminated(pid, waitpidStat);

  if(pid == m_processForkerPid) {
    handleReapedProcessForker(pid, waitpidStat);
  } else {
    log::Param params[] = {log::Param("pid", pid)};
    m_log(LOG_ERR, "Reaped process was unknown", params);
  }
}

//------------------------------------------------------------------------------
// handleReapedProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::handleReapedProcessForker(
  const pid_t pid, const int waitpidStat) throw() {
  log::Param params[] = {
    log::Param("processForkerPid", pid)};
  m_log(LOG_INFO, "Handling reaped ProcessForker", params);
  m_processForkerPid = 0;
}

//------------------------------------------------------------------------------
// logChildProcessTerminated
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::logChildProcessTerminated(
  const pid_t pid, const int waitpidStat) throw() {
  std::list<log::Param> params;
  params.push_back(log::Param("terminatedPid", pid));

  if(WIFEXITED(waitpidStat)) {
    params.push_back(log::Param("WEXITSTATUS", WEXITSTATUS(waitpidStat)));
  }

  if(WIFSIGNALED(waitpidStat)) {
    params.push_back(log::Param("WTERMSIG", WTERMSIG(waitpidStat)));
  }

  if(WCOREDUMP(waitpidStat)) {
    params.push_back(log::Param("WCOREDUMP", "true"));
  } else {
    params.push_back(log::Param("WCOREDUMP", "false"));
  }

  if(WIFSTOPPED(waitpidStat)) {
    params.push_back(log::Param("WSTOPSIG", WSTOPSIG(waitpidStat)));
  }

  if(WIFCONTINUED(waitpidStat)) {
    params.push_back(log::Param("WIFCONTINUED", "true"));
  } else {
    params.push_back(log::Param("WIFCONTINUED", "false"));
  }

  m_log(LOG_INFO, "Child process terminated", params);
}

//------------------------------------------------------------------------------
// requestVdqmToReleaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::requestVdqmToReleaseDrive(
  const utils::DriveConfig &driveConfig, const pid_t pid) {
  std::list<log::Param> params;
  try {
    const bool forceUnmount = true;

    params.push_back(log::Param("pid", pid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));
    params.push_back(log::Param("forceUnmount", forceUnmount));

    m_vdqm.releaseDrive(m_hostName, driveConfig.unitName, driveConfig.dgn,
      forceUnmount, pid);
    m_log(LOG_INFO, "Requested vdqm to release drive", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to request vdqm to release drive: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// notifyVdqmTapeUnmounted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::notifyVdqmTapeUnmounted(
  const utils::DriveConfig &driveConfig, const std::string &vid,
  const pid_t pid) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("pid", pid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("vid", vid));
    params.push_back(log::Param("dgn", driveConfig.dgn));

    m_vdqm.tapeUnmounted(m_hostName, driveConfig.unitName, driveConfig.dgn,
      vid);
    m_log(LOG_INFO, "Notified vdqm that a tape was unmounted", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed notify vdqm that a tape was unmounted: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveDownInVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::TapeDaemon::setDriveDownInVdqm(
  const pid_t pid, const utils::DriveConfig &driveConfig) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", pid));

  try {
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));

    m_vdqm.setDriveDown(m_hostName, driveConfig.unitName, driveConfig.dgn);
    m_log(LOG_INFO, "Set tape-drive down in vdqm", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set tape-drive down in vdqm: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getDataTransferConf
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DataTransferSession::CastorConf
  castor::tape::tapeserver::daemon::TapeDaemon::getDataTransferConf() {
  DataTransferSession::CastorConf castorConf;
  common::CastorConfiguration &config =
    common::CastorConfiguration::getConfig();
  castorConf.rtcopydBufsz = config.getConfEntInt(
    "RTCOPYD", "BUFSZ", (uint32_t)RTCP_BUFSZ, &m_log);
  castorConf.rtcopydNbBufs = config.getConfEntInt<uint32_t>(
    "RTCOPYD", "NB_BUFS", &m_log);
  castorConf.tapeBadMIRHandlingRepair = config.getConfEntString(
    "TAPE", "BADMIR_HANDLING", "CANCEL", &m_log);
  castorConf.tapebridgeBulkRequestMigrationMaxBytes = config.getConfEntInt(
    "TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXBYTES",
    (uint64_t)server::TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES, &m_log);
  castorConf.tapebridgeBulkRequestMigrationMaxFiles = config.getConfEntInt(
    "TAPEBRIDGE", "BULKREQUESTMIGRATIONMAXFILES",
    (uint64_t)server::TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES, &m_log);
  castorConf.tapebridgeBulkRequestRecallMaxBytes = config.getConfEntInt(
    "TAPEBRIDGE", "BULKREQUESTRECALLMAXBYTES",
    (uint64_t)server::TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES, &m_log);
  castorConf.tapebridgeBulkRequestRecallMaxFiles = config.getConfEntInt(
    "TAPEBRIDGE", "BULKREQUESTRECALLMAXFILES",
    (uint64_t)server::TAPEBRIDGE_BULKREQUESTRECALLMAXFILES, &m_log);
  castorConf.tapebridgeMaxBytesBeforeFlush = config.getConfEntInt(
    "TAPEBRIDGE", "MAXBYTESBEFOREFLUSH",
    (uint64_t)server::TAPEBRIDGE_MAXBYTESBEFOREFLUSH, &m_log);
  castorConf.tapebridgeMaxFilesBeforeFlush = config.getConfEntInt(
    "TAPEBRIDGE", "MAXFILESBEFOREFLUSH",
    (uint64_t)server::TAPEBRIDGE_MAXFILESBEFOREFLUSH, &m_log);
  castorConf.tapeserverdDiskThreads = config.getConfEntInt(
    "RTCPD", "THREAD_POOL", (uint32_t)RTCPD_THREAD_POOL, &m_log);
  castorConf.tapeserverdRemoteFileProtocol = config.getConfEntString(
    "TAPESERVERD", "REMOTEFILEPROTOCOL", "RFIO", &m_log);
  castorConf.xrootPrivateKey = config.getConfEntString(
    "XROOT", "PRIVATEKEY", "/opt/xrootd/keys/key.pem", &m_log);

  return castorConf;
}

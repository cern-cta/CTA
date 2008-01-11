/******************************************************************************
 *                      VdqmServer.cpp
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
 * @author castor dev team
 *****************************************************************************/
 
 
// Include Files
#include <string>

#include "Cgetopt.h"
#include "Cinit.h"
#include "Cuuid.h"
#include "Cpool_api.h"
#include "errno.h"

#define VDQMSERV

#include <net.h>
#include <vdqm_constants.h>	//e.g. Magic Number of old vdqm protocol

#include "castor/server/SignalThreadPool.hpp"
#include "castor/server/TCPListenerThreadPool.hpp"
#include "castor/vdqm/DriveSchedulerThread.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/RequestHandlerThread.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmServer.hpp"


//------------------------------------------------------------------------------
// main method
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {

  castor::vdqm::VdqmServer       server;
  castor::server::BaseThreadPool *requestHandlerThreadPool = NULL;
  castor::server::BaseThreadPool *driveSchedulerThreadPool = NULL;


  //-----------------------
  // Parse the command line
  //-----------------------

  server.parseCommandLine(argc, argv);


  //------------------------
  // Create the thread pools
  //------------------------

  server.addThreadPool(
    new castor::server::TCPListenerThreadPool("RequestHandlerThreadPool",
      new castor::vdqm::RequestHandlerThread(), server.getListenPort()));

  server.addThreadPool(
    new castor::server::SignalThreadPool("DriveSchedulerThreadPool",
      new castor::vdqm::DriveSchedulerThread()));


  //----------------------------------------------
  // Set the number of threads in each thread pool
  //----------------------------------------------

  requestHandlerThreadPool = server.getThreadPool('R');
  if(requestHandlerThreadPool == NULL) {
    std::cerr << "Failed to get RequestHandlerThreadPool" << std::endl;
    return 1;
  }

  requestHandlerThreadPool->setNbThreads(
    server.getRequestHandlerThreadNumber());

  driveSchedulerThreadPool = server.getThreadPool('D');
  if(driveSchedulerThreadPool == NULL) {
    std::cerr << "Failed to get DriveSchedulerThreadPool" << std::endl;
    return 1;
  }

  driveSchedulerThreadPool->setNbThreads(
    server.getDriveSchedulerThreadNumber());

  try {

    //-----------------
    // Start the server
    //-----------------

    server.start();

  } catch (castor::exception::Exception e) {
    std::cerr << "Failed to start VDQM server : "
              << sstrerror(e.code()) << std::endl
              << e.getMessage().str() << std::endl;

    return 1;
  } catch (...) {
    std::cerr << "Failed to start VDQM server : Caught general exception!"
      << std::endl;
    
    return 1;
  }
  
  return 0;
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::VdqmServer::VdqmServer()
  throw():
  castor::server::BaseDaemon("Vdqm"),
  m_requestHandlerThreadNumber(1),
  m_driveSchedulerThreadNumber(1)
{
  initDlf();
}


//------------------------------------------------------------------------------
// initDlf
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::initDlf()
  throw()
{
  castor::dlf::Message messages[] = {
    {VDQM_NULL, " - "},
    {VDQM_NEW_REQUEST_ARRIVAL, "New Request Arrival"},
    {VDQM_FAILED_TO_GET_CNVS_FOR_DB, "Could not get Conversion Service for Database"},
    {VDQM_FAILED_TO_GET_CNVS_FOR_STREAMING, "Could not get Conversion Service for Streaming"},
    {VDQM_EXCEPTION_SERVER_STOPPING, "Exception caught : server is stopping"},
    {VDQM_EXCEPTION_IGNORED, "Exception caught : ignored"},
    {VDQM_REQUEST_HAS_OLD_PROTOCOL_MAGIC_NB, "Request has MagicNumber from old VDQM Protocol"},
    {VDQM_FAILED_SOCK_READ, "Unable to read Request from socket"},
    {VDQM_FAILED_INIT_DRIVE_DEDICATION_THREAD, "Unable to initialize TapeRequestDedicationHandler thread"},
    {VDQM_EXCEPTION, "Exception caught"},
    {VDQM_SEND_REPLY_TO_CLIENT, "Sending reply to client"},
    {VDQM_FAILED_SEND_ACK_TO_CLIENT, "Unable to send Ack to client"},
    {VDQM_REQUEST_STORED_IN_DB, "Request stored in DB"},
    {VDQM_WRONG_MAGIC_NB, "Wrong Magic number"},
    {VDQM_HANDLE_OLD_VDQM_REQUEST_TYPE, "Handle old vdqm request type"},
    {VDQM_ADMIN_REQUEST, "ADMIN request"},
    {VDQM_NEW_VDQM_REQUEST, "New VDQM request"},
    {VDQM_HANDLE_REQUEST_TYPE_ERROR, "Handle Request type error"},
    {VDQM_SERVER_SHUTDOWN_REQUSTED, "shutdown server requested"},
    {VDQM_HANDLE_VOL_REQ, "Handle VDQM_VOL_REQ"},
    {VDQM_HANDLE_DRV_REQ, "Handle VDQM_DRV_REQ"},
    {VDQM_HANDLE_DEL_VOLREQ, "Handle VDQM_DEL_VOLREQ"},
    {VDQM_HANDLE_DEL_DRVREQ, "Handle VDQM_DEL_DRVREQ"},
    {VDQM_OLD_VDQM_VOLREQ_PARAMS, "The parameters of the old vdqm VolReq Request"},
    {VDQM_REQUEST_PRIORITY_CHANGED, "Request priority changed"},
    {VDQM_HANDLE_VDQM_PING, "Handle VDQM_PING"},
    {VDQM_QUEUE_POS_OF_TAPE_REQUEST, "Queue position of TapeRequest"},
    {VDQM_SEND_BACK_VDQM_PING, "Send VDQM_PING back to client"},
    {VDQM_TAPE_REQUEST_ANDCLIENT_ID_REMOVED, "TapeRequest and its ClientIdentification removed"},
    {VDQM_REQUEST_DELETED_FROM_DB, "Request deleted from DB"},
    {VDQM_NO_VDQM_COMMIT_FROM_CLIENT, "Client didn't send a VDQM_COMMIT => Rollback of request in db"},
    {VDQM_VERIFY_REQUEST_NON_EXISTANT, "Verify that the request doesn't exist, by calling IVdqmSvc->checkTapeRequest"},
    {VDQM_STORE_REQUEST_IN_DB, "Try to store Request into the data base"},
    {VDQM_OLD_VDQM_DRV_REQ_PARAMS, "The parameters of the old vdqm DrvReq Request"},
    {VDQM_CREATE_DRIVE_IN_DB, "Create new TapeDrive in DB"},
    {VDQM_DESIRED_OLD_PROTOCOL_CLIENT_STATUS, "The desired \"old Protocol\" status of the client"},
    {VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS, "The actual \"new Protocol\" status of the client"},
    {VDQM_REMOVE_OLD_TAPE_REQUEST_FROM_DB, "Remove old TapeRequest from db"},
    {VDQM_WAIT_DOWN_REQUEST_FROM_TPDAEMON_CLIENT, "WAIT DOWN request from tpdaemon client"},
    {VDQM_ASSIGN_TAPE_REQUEST_TO_JOB_ID, "Assign of tapeRequest to jobID"},
    {VDQM_LOCAL_ASSIGN_TO_JOB_ID, "Local assign to jobID"},
    {VDQM_INCONSISTENT_RELEASE_ON_DRIVE, "Inconsistent release on tape drive"},
    {VDQM_CLIENT_REQUESTED_FORCED_UNMOUNT, "client has requested a forced unmount."},
    {VDQM_DRIVE_STATUS_UNKNOWN_FORCE_UNMOUNT, "tape drive in STATUS_UNKNOWN status. Force unmount!"},
    {VDQM_NO_TAPE_REQUEST_FOR_MOUNTED_TAPE, "No tape request left for mounted tape"},
    {VDQM_UPDATE_REPRESENTATION_IN_DB, "Update of representation in DB"},
    {VDQM_NO_FREE_DRIVE_OR_NO_TAPE_REQUEST_IN_DB, "No free TapeDrive, or no TapeRequest in the db"},
    {VDQM_GET_TAPE_INFO_FROM_VMGR, "Try to get information about the tape from the VMGR daemon"},
    {VDQM_RTCOPYDCONNECTION_ERRMSG_TOO_LARGE, "RTCopyDConnection: Too large errmsg buffer requested"},
    {VDQM_RTCOPYDCONNECTION_RTCOPY_ERROR, "RTCopyDConnection: rtcopy daemon returned an error"},
    {VDQM_TAPE_REQUEST_DEDICATION_HANDLER_RUN_EXCEPTION, "Exception caught in TapeRequestDedicationHandler::run()"},
    {VDQM_HANDLE_OLD_VDQM_REQUEST_ROLLBACK, "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request"},
    {VDQM_HANDLE_VOL_MOUNT_STATUS_MOUNTED, "TapeDriveStatusHandler::handleVolMountStatus(): Tape mounted in tapeDrive"},
    {VDQM_HANDLE_VDQM_DEDICATE_DRV, "Handle VDQM_DEDICATE_DRV"},
    {VDQM_HANDLE_VDQM_GET_VOLQUEUE, "Handle VDQM_GET_VOLQUEUE"},
    {VDQM_HANDLE_VDQM_GET_DRVQUEUE, "Handle VDQM_GET_DRVQUEUE"},
    {VDQM_HANDLE_DEDICATION_EXCEPTION, "Exception caught in TapeRequestDedicationHandler::handleDedication()"},
    {VDQM_SEND_SHOWQUEUES_INFO, "Send information for showqueues command"},
    {VDQM_THREAD_POOL_CREATED, "Thread pool created"},
    {VDQM_THREAD_POOL_CREATION_ERROR, "Thread pool creation error"},
    {VDQM_ASSIGN_REQUEST_TO_POOL_ERROR, "Error while assigning request to pool"},
    {VDQM_START_TAPE_TO_TAPE_DRIVE_DEDICATION_THREAD, "Start tape to tape drive dedication thread"},
    {VDQM_DEDICATION_THREAD_POOL_CREATED, "Dedication thread pool created"},
    {VDQM_DEDICATION_REQUEST_EXCEPTION, "Exception caught in TapeRequestDedicationHandler::dedicationRequest()"},
    {VDQM_NO_TAPE_DRIVE_TO_COMMIT_TO_RTCPD, "No TapeDrive object to commit to RTCPD"},
    {VDQM_FOUND_QUEUED_TAPE_REQUEST_FOR_MOUNTED_TAPE, "Found a queued tape request for mounted tape"},
    {VDQM_HANDLE_OLD_VDQM_REQUEST_WAITING_FOR_CLIENT_ACK, "VdqmServer::handleOldVdqmRequest(): waiting for client acknowledge"},
    {VDQM_TAPE_REQUEST_NOT_FOUND_IN_DB, "Couldn't find the tape request in db. Maybe it is already deleted?"},
    {VDQM_DBVDQMSVC_GETSVC, "Could not get DbVdqmSvc"},
    {VDQM_MATCHTAPE2TAPEDRIVE_ERROR, "Error occured when determining if there is matching free drive and waiting request"},
    {VDQM_DRIVE_ALLOCATION_ERROR, "Error occurred whilst allocating a free drive to a tape request"},
    {VDQM_HANDLE_REQUEST_EXCEPT, "Exception raised by castor::vdqm::VdqmServer::handleRequest"},
    {-1, ""}
  }; // castor::dlf::Message messages[]

  dlfInit(messages);
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::parseCommandLine(int argc, char *argv[])
  throw() {

  static struct Coptions longopts[] = {
    {"foreground"           , NO_ARGUMENT      , NULL, 'f'},
    {"config"               , REQUIRED_ARGUMENT, NULL, 'c'},
    {"help"                 , NO_ARGUMENT      , NULL, 'h'},
    {"requestHandlerThreads", REQUIRED_ARGUMENT, NULL, 'n'},
    {"driveSchedulerThreads", REQUIRED_ARGUMENT, NULL, 'd'},
    {NULL                   , 0                , NULL,  0 }
  };

  Coptind = 1;
  Copterr = 0;

  char c;
  while ((c = Cgetopt_long (argc, argv, "fn:d:", longopts, NULL)) != -1) {
    switch (c) {
    case 'f':
      m_foreground = true;
      break;
    case 'c':
      setenv("PATH_CONFIG", Coptarg, 1);
      break;
    case 'h':
      help(argv[0]);
      exit(0);
    case 'n':
      m_requestHandlerThreadNumber = atoi(Coptarg);
      break;
    case 'd':
      m_driveSchedulerThreadNumber = atoi(Coptarg);

      if(m_driveSchedulerThreadNumber != 1) {
        std::cerr << "Error: More than one drive scheduler thread is not yet "
          << "supported" << std::endl << std::endl;
        exit(1);
      }

      break;
    case '?':
      std::cerr << "Error: Unknown command-line option: " << (char)Coptopt
        << std::endl << std::endl;
      help(argv[0]);
      exit(1);
    default:
      std::cerr << "Internal error: Unknown return value from Cgetopt_long"
        << std::endl << std::endl;
      exit(1);
    }
  }

  if(Coptind > argc) {
    std::cerr << "Internal error.  Invalid value for Coptind: " << Coptind
      << std::endl;
    exit(1);
  }

  // Best to abort if there is some extra text on the command-line which has
  // not been parsed as it could indicate that a valid option never got parsed
  if(Coptind < argc)
  {
      std::cerr << "Error:  Unexpected command-line argument: "
        << argv[Coptind] << std::endl << std::endl;
      help(argv[0]);
      exit(1);
  }
}


//------------------------------------------------------------------------------
// help
//------------------------------------------------------------------------------
void castor::vdqm::VdqmServer::help(std::string programName)
  throw() {
  std::cerr << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t--foreground            or -f     \tRemain in the Foreground\n"
    "\t--config <config-file>  or -c     \tConfiguration file\n"
    "\t--help                  or -h     \tPrint this help and exit\n"
    "\t--requestHandlerThreads or -n num \tDefault 1\n"
    "\t--driveSchedulerThreads or -d num \tDefault 1\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// getListenPort
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getListenPort()
{
  return VDQM_PORT;
}


//------------------------------------------------------------------------------
// getRequestHandlerThreadNb
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getRequestHandlerThreadNumber()
{
  return m_requestHandlerThreadNumber;
}


//------------------------------------------------------------------------------
// getDriveSchedulerThreadNb
//------------------------------------------------------------------------------
int castor::vdqm::VdqmServer::getDriveSchedulerThreadNumber()
{
  return m_driveSchedulerThreadNumber;
}

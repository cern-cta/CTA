/******************************************************************************
 *                      VdqmDlfMessages.hpp
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

#ifndef CASTOR_VDQM_VDQMDLFMESSAGE_CONSTANTS_HPP
#define CASTOR_VDQM_VDQMDLFMESSAGE_CONSTANTS_HPP 1


namespace castor {

  namespace vdqm {
  	
    enum VdqmDlfMessages {
      VDQM_NULL=0, /* " - " */
      VDQM_NEW_REQUEST_ARRIVAL=1, /* "New Request Arrival" */
      VDQM_FAILED_TO_GET_CNVS_FOR_DB=2, /* "Could not get Conversion Service for Database" */
      VDQM_FAILED_TO_GET_CNVS_FOR_STREAMING=3, /* "Could not get Conversion Service for Streaming" */
      VDQM_EXCEPTION_SERVER_STOPPING=4, /* "Exception caught : server is stopping" */
      VDQM_EXCEPTION_IGNORED=5, /* "Exception caught : ignored" */
      VDQM_REQUEST_HAS_OLD_PROTOCOL_MAGIC_NB=6, /* "Request has MagicNumber from old VDQM Protocol" */
      VDQM_FAILED_SOCK_READ=7, /* "Unable to read Request from socket" */
      VDQM_FAILED_INIT_DRIVE_DEDICATION_THREAD=8, /* "Unable to initialize TapeRequestDedicationHandler thread" */
      VDQM_EXCEPTION=9, /* "Exception caught" */
      VDQM_SEND_REPLY_TO_CLIENT=10, /* "Sending reply to client" */
      VDQM_FAILED_SEND_ACK_TO_CLIENT=11, /* "Unable to send Ack to client" */
      VDQM_REQUEST_STORED_IN_DB=12, /* "Request stored in DB" */
      VDQM_WRONG_MAGIC_NB=13, /* "Wrong Magic number" */
      VDQM_HANDLE_OLD_VDQM_REQUEST_TYPE=14, /* "Handle old vdqm request type" */
      VDQM_ADMIN_REQUEST=15, /* "ADMIN request" */
      VDQM_NEW_VDQM_REQUEST=16, /* "New VDQM request" */
      VDQM_HANDLE_REQUEST_TYPE_ERROR=17, /* "Handle Request type error" */
      VDQM_SERVER_SHUTDOWN_REQUSTED=18, /* "shutdown server requested" */
      VDQM_HANDLE_VOL_REQ=19, /* "Handle VDQM_VOL_REQ" */
      VDQM_HANDLE_DRV_REQ=20, /* "Handle VDQM_DRV_REQ" */
      VDQM_HANDLE_DEL_VOLREQ=21, /* "Handle VDQM_DEL_VOLREQ" */
      VDQM_HANDLE_DEL_DRVREQ=22, /* "Handle VDQM_DEL_DRVREQ" */
      VDQM_OLD_VDQM_VOLREQ_PARAMS=23, /* "The parameters of the old vdqm VolReq Request" */
      VDQM_REQUEST_PRIORITY_CHANGED=24, /* "Request priority changed" */
      VDQM_HANDLE_VDQM_PING=25, /* "Handle VDQM_PING" */
      VDQM_QUEUE_POS_OF_TAPE_REQUEST=26, /* "Queue position of TapeRequest" */
      VDQM_SEND_BACK_VDQM_PING=27, /* "Send VDQM_PING back to client" */
      VDQM_TAPE_REQUEST_ANDCLIENT_ID_REMOVED=28, /* "TapeRequest and its ClientIdentification removed" */
      VDQM_REQUEST_DELETED_FROM_DB=29, /* "Request deleted from DB" */
      VDQM_NO_VDQM_COMMIT_FROM_CLIENT=30, /* "Client didn't send a VDQM_COMMIT => Rollback of request in db" */
      VDQM_VERIFY_REQUEST_NON_EXISTANT=31, /* "Verify that the request doesn't exist, by calling IVdqmSvc->checkTapeRequest" */
      VDQM_STORE_REQUEST_IN_DB=32, /* "Try to store Request into the data base" */
      VDQM_OLD_VDQM_DRV_REQ_PARAMS=33, /* "The parameters of the old vdqm DrvReq Request" */
      VDQM_CREATE_DRIVE_IN_DB=34, /* "Create new TapeDrive in DB" */
      VDQM_DESIRED_OLD_PROTOCOL_CLIENT_STATUS=35, /* "The desired \"old Protocol\" status of the client" */
      VDQM_ACTUAL_NEW_PROTOCOL_CLIENT_STATUS=36, /* "The actual \"new Protocol\" status of the client" */
      VDQM_REMOVE_OLD_TAPE_REQUEST_FROM_DB=37, /* "Remove old TapeRequest from db" */
      VDQM_WAIT_DOWN_REQUEST_FROM_TPDAEMON_CLIENT=38, /* "WAIT DOWN request from tpdaemon client" */
      VDQM_ASSIGN_TAPE_REQUEST_TO_JOB_ID=39, /* "Assign of tapeRequest to jobID" */
      VDQM_LOCAL_ASSIGN_TO_JOB_ID=40, /* "Local assign to jobID" */
      VDQM_INCONSISTENT_RELEASE_ON_DRIVE=41, /* "Inconsistent release on tape drive" */
      VDQM_CLIENT_REQUESTED_FORCED_UNMOUNT=42, /* "client has requested a forced unmount." */
      VDQM_DRIVE_STATUS_UNKNOWN_FORCE_UNMOUNT=43, /* "tape drive in STATUS_UNKNOWN status. Force unmount!" */
      VDQM_NO_TAPE_REQUEST_FOR_MOUNTED_TAPE=44, /* "No tape request left for mounted tape" */
      VDQM_UPDATE_REPRESENTATION_IN_DB=45, /* "Update of representation in DB" */
      VDQM_NO_FREE_DRIVE_OR_NO_TAPE_REQUEST_IN_DB=46, /* "No free TapeDrive, or no TapeRequest in the db" */
      VDQM_GET_TAPE_INFO_FROM_VMGR=47, /* "Try to get information about the tape from the VMGR daemon" */
      VDQM_RTCOPYDCONNECTION_ERRMSG_TOO_LARGE=48, /* "RTCopyDConnection: Too large errmsg buffer requested" */
      VDQM_RTCOPYDCONNECTION_RTCOPY_ERROR=49, /* "RTCopyDConnection: rtcopy daemon returned an error" */
      VDQM_TAPE_REQUEST_DEDICATION_HANDLER_RUN_EXCEPTION=50, /* "Exception caught in TapeRequestDedicationHandler::run()" */
      VDQM_HANDLE_OLD_VDQM_REQUEST_ROLLBACK=51, /* "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request" */
      VDQM_HANDLE_VOL_MOUNT_STATUS_MOUNTED=52, /* "TapeDriveStatusHandler::handleVolMountStatus(): Tape mounted in tapeDrive" */
      VDQM_HANDLE_VDQM_DEDICATE_DRV=53, /* "Handle VDQM_DEDICATE_DRV" */
      VDQM_HANDLE_VDQM_GET_VOLQUEUE=54, /* "Handle VDQM_GET_VOLQUEUE" */
      VDQM_HANDLE_VDQM_GET_DRVQUEUE=55, /* "Handle VDQM_GET_DRVQUEUE" */
      VDQM_HANDLE_DEDICATION_EXCEPTION=56, /* "Exception caught in TapeRequestDedicationHandler::handleDedication()" */
      VDQM_SEND_SHOWQUEUES_INFO=57, /* "Send information for showqueues command" */
      VDQM_THREAD_POOL_CREATED=58, /* "Thread pool created" */
      VDQM_THREAD_POOL_CREATION_ERROR=59, /* "Thread pool creation error" */
      VDQM_ASSIGN_REQUEST_TO_POOL_ERROR=60, /* "Error while assigning request to pool" */
      VDQM_START_TAPE_TO_TAPE_DRIVE_DEDICATION_THREAD=61, /* "Start tape to tape drive dedication thread" */
      VDQM_DEDICATION_THREAD_POOL_CREATED=62, /* "Dedication thread pool created" */
      VDQM_DEDICATION_REQUEST_EXCEPTION=63, /* "Exception caught in TapeRequestDedicationHandler::dedicationRequest()" */
      VDQM_NO_TAPE_DRIVE_TO_COMMIT_TO_RTCPD=64, /* "No TapeDrive object to commit to RTCPD" */
      VDQM_FOUND_QUEUED_TAPE_REQUEST_FOR_MOUNTED_TAPE=65, /* "Found a queued tape request for mounted tape" */
      VDQM_HANDLE_OLD_VDQM_REQUEST_WAITING_FOR_CLIENT_ACK=66, /* "VdqmServer::handleOldVdqmRequest(): waiting for client acknowledge" */
      VDQM_TAPE_REQUEST_NOT_FOUND_IN_DB=67, /* "Couldn't find the tape request in db. Maybe it is already deleted?" */
      VDQM_DBVDQMSVC_GETSVC=68, /* "Could not get DbVdqmSvc" */
      VDQM_MATCHTAPE2TAPEDRIVE_ERROR=69, /* "Error occured when determining if there is matching free drive and waiting request" */
      VDQM_DRIVE_ALLOCATION_ERROR=70, /* "Error occurred whilst allocating a free drive to a tape request" */
      VDQM_HANDLE_REQUEST_EXCEPT=71, /* "Exception raised by castor::vdqm::VdqmServer::handleRequest" */
      VDQM_SEND_RTCPD_JOB=72, /* "Sending job to RTCPD" */
      VDQM_DRIVE_STATE_TRANSITION=73, /* "Tape drive state transition" */
      VDQM_DRIVE_ALLOCATED=74, /* "Tape drive allocated" */
      VDQM_INVALIDATED_DRIVE_ALLOCATION=75, /* "Invalidated tape drive allocation" */
      VDQM_INVALIDATED_REUSE_OF_DRIVE_ALLOCATION=76, /* "Invalidated reuse of tape drive allocation" */
      VDQM_REMOVING_TAPE_REQUEST=77, /* "Removing tape request" */
      VDQM_IGNORED_RTCPD_JOB_SUBMISSION=78, /* "Ignored successful submission of RTCPD job" */
      VDQM_IGNORED_FAILED_RTCPD_JOB_SUBMISSION=79, /* "Ignored failed submission of RTCPD job" */
      VDQM_TAPE_REQUEST_NOT_IN_QUEUE=80, /* "Tape request not in queue" */
      VDQM_MOUNT_WITHOUT_VOL_REQ=81, /* "Volume mounted without a corresponding volume request" */
      VDQM_MAGIC2_VOL_PRIORITY_ROLLBACK=82, /* "Rollback of whole VDQM_MAGIC2 vdqmVolPriority request" */
      VDQM_HANDLE_VDQM2_VOL_PRIORITY=83, /* "Handle VDQM2_VOL_PRIORITY" */
      VDQM_DELETE_VOL_PRIORITY=84 /* "Delete volume priority" */
    }; // enum VdqmDlfMessages

  } // namespace vdqm

} // namespace castor


#endif // CASTOR_VDQM_VDQMDLFMESSAGE_CONSTANTS_HPP

/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "castor/messages/Constants.hpp"

//------------------------------------------------------------------------------
// msgTypeToString
//------------------------------------------------------------------------------
const char *castor::messages::msgTypeToString(const MsgType msgType) noexcept {
  switch(msgType) {
  case MSG_TYPE_NONE:
    return "None";
  case MSG_TYPE_EXCEPTION:
    return "Exception";
  case MSG_TYPE_FORKCLEANER:
    return "ForkCleaner";
  case MSG_TYPE_FORKDATATRANSFER:
    return "ForkDataTransfer";
  case MSG_TYPE_FORKLABEL:
    return "ForkLabel";
  case MSG_TYPE_FORKSUCCEEDED:
    return "ForkSucceeded";
  case MSG_TYPE_HEARTBEAT:
    return "Heartbeat";
  case MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY:
    return "MigrationJobFromTapeGateway";
  case MSG_TYPE_MIGRATIONJOBFROMWRITETP:
    return "MigrationJobFromWriteTp";
  case MSG_TYPE_NBFILESONTAPE:
    return "NbFilesOnTape";
  case MSG_TYPE_PROCESSCRASHED:
    return "ProcessCrashed";
  case MSG_TYPE_PROCESSEXITED:
    return "ProcessExited";
  case MSG_TYPE_RECALLJOBFROMREADTP:
    return "RecallJobFromReadTp";
  case MSG_TYPE_RECALLJOBFROMTAPEGATEWAY:
    return "RecallJobFromTapeGAteway";
  case MSG_TYPE_RETURNVALUE:
    return "ReturnValue";
  case MSG_TYPE_STOPPROCESSFORKER:
    return "StopProcessForker";
  case MSG_TYPE_TAPEMOUNTEDFORMIGRATION:
    return "TapeMountedForMigration";
  case MSG_TYPE_TAPEMOUNTEDFORRECALL:
    return "TapeMountedForRecall";
  case MSG_TYPE_LABELERROR:
    return "LabelError";
  case MSG_TYPE_ACSMOUNTTAPEREADONLY:
    return "AcsMountTapeReadOnly";
  case MSG_TYPE_ACSMOUNTTAPEREADWRITE:
    return "AcsMountTapeReadWrite";        
  case MSG_TYPE_ACSDISMOUNTTAPE:
    return "AcsDismountTape";
  case MSG_TYPE_ACSFORCEDISMOUNTTAPE:
    return "AcsForceDismountTape";
  case MSG_TYPE_ADDLOGPARAMS:
    return "AddLogParams";
  case MSG_TYPE_DELETELOGPARAMS:
    return "DeleteLogParams";
  default:
    return "Unknown";
  }
} // msgTypeToString()

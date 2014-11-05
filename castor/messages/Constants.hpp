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

#pragma once

namespace castor {
namespace messages {

enum ProtocolType {
  PROTOCOL_TYPE_NONE,
  PROTOCOL_TYPE_TAPE
};

enum MsgType {
  MSG_TYPE_NONE,
  MSG_TYPE_EXCEPTION,
  MSG_TYPE_FORKCLEANER,
  MSG_TYPE_FORKDATATRANSFER,
  MSG_TYPE_FORKLABEL,
  MSG_TYPE_FORKSUCCEEDED,
  MSG_TYPE_HEARTBEAT,
  MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY,
  MSG_TYPE_MIGRATIONJOBFROMWRITETP,
  MSG_TYPE_NBFILESONTAPE,
  MSG_TYPE_PROCESSCRASHED,
  MSG_TYPE_PROCESSEXITED,
  MSG_TYPE_RECALLJOBFROMREADTP,
  MSG_TYPE_RECALLJOBFROMTAPEGATEWAY,
  MSG_TYPE_RETURNVALUE,
  MSG_TYPE_STOPPROCESSFORKER,
  MSG_TYPE_TAPEMOUNTEDFORMIGRATION,
  MSG_TYPE_TAPEMOUNTEDFORRECALL,
  MSG_TYPE_TAPEUNMOUNTSTARTED,
  MSG_TYPE_TAPEUNMOUNTED,
  MSG_TYPE_LABELERROR,
  MSG_TYPE_ACSMOUNTTAPEREADONLY,
  MSG_TYPE_ACSMOUNTTAPEREADWRITE,
  MSG_TYPE_ACSDISMOUNTTAPE,
  MSG_TYPE_ADDLOGPARAMS,
  MSG_TYPE_DELETELOGPARAMS
};

enum ProtocolVersion {
  PROTOCOL_VERSION_NONE,
  PROTOCOL_VERSION_1
};

/**
 * Returns the string representation of the specified message type.
 *
 * This method is thread safe because it only returns pointers to string
 * literals.
 *
 * In the case where the specified message type is unknown this method does not
 * throw an exception, instead is returns a string literal that explains the
 * message type is unknown.
 */
const char *msgTypeToString(const MsgType msgType) throw();

} // namespace messages
} // namespace castor

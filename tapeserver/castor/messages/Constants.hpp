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
  /*  0 */ MSG_TYPE_NONE,
  /*  1 */ MSG_TYPE_EXCEPTION,
  /*  2 */ MSG_TYPE_FORKCLEANER,
  /*  3 */ MSG_TYPE_FORKDATATRANSFER,
  /*  4 */ MSG_TYPE_FORKLABEL,
  /*  5 */ MSG_TYPE_FORKSUCCEEDED,
  /*  6 */ MSG_TYPE_HEARTBEAT,
  /*  7 */ MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY,
  /*  8 */ MSG_TYPE_MIGRATIONJOBFROMWRITETP,
  /*  9 */ MSG_TYPE_NBFILESONTAPE,
  /* 10 */ MSG_TYPE_PROCESSCRASHED,
  /* 11 */ MSG_TYPE_PROCESSEXITED,
  /* 12 */ MSG_TYPE_RECALLJOBFROMREADTP,
  /* 13 */ MSG_TYPE_RECALLJOBFROMTAPEGATEWAY,
  /* 14 */ MSG_TYPE_RETURNVALUE,
  /* 15 */ MSG_TYPE_STOPPROCESSFORKER,
  /* 16 */ MSG_TYPE_TAPEMOUNTEDFORMIGRATION,
  /* 17 */ MSG_TYPE_TAPEMOUNTEDFORRECALL,
  /* 18 */ MSG_TYPE_TAPEUNMOUNTSTARTED,
  /* 19 */ MSG_TYPE_TAPEUNMOUNTED,
  /* 20 */ MSG_TYPE_LABELERROR,
  /* 21 */ MSG_TYPE_ACSMOUNTTAPEREADONLY,
  /* 22 */ MSG_TYPE_ACSMOUNTTAPEREADWRITE,
  /* 23 */ MSG_TYPE_ACSDISMOUNTTAPE,
  /* 24 */ MSG_TYPE_ACSFORCEDISMOUNTTAPE,
  /* 25 */ MSG_TYPE_ADDLOGPARAMS,
  /* 26 */ MSG_TYPE_DELETELOGPARAMS,
  /* 27 */ MSG_TYPE_ARCHIVEJOBFROMCTA,
  /* 28 */ MSG_TYPE_RETRIEVEJOBFROMCTA
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

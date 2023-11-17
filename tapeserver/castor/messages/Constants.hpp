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

#pragma once

namespace castor::messages {

enum ProtocolType {
  PROTOCOL_TYPE_NONE,
  PROTOCOL_TYPE_TAPE
};

enum MsgType {
  /*  0 */ MSG_TYPE_NONE                        =  0,
  /*  1 */ MSG_TYPE_EXCEPTION                   =  1,
  /*  2 */ MSG_TYPE_FORKCLEANER                 =  2,
  /*  3 */ MSG_TYPE_FORKDATATRANSFER            =  3,
  /*  4 */ MSG_TYPE_FORKLABEL                   =  4,
  /*  5 */ MSG_TYPE_FORKSUCCEEDED               =  5,
  /*  6 */ MSG_TYPE_HEARTBEAT                   =  6,
  /*  7 */ MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY =  7,
  /*  8 */ MSG_TYPE_MIGRATIONJOBFROMWRITETP     =  8,
  /*  9 */ MSG_TYPE_NBFILESONTAPE               =  9,
  /* 10 */ MSG_TYPE_PROCESSCRASHED              = 10,
  /* 11 */ MSG_TYPE_PROCESSEXITED               = 11,
  /* 12 */ MSG_TYPE_RECALLJOBFROMREADTP         = 12,
  /* 13 */ MSG_TYPE_RECALLJOBFROMTAPEGATEWAY    = 13,
  /* 14 */ MSG_TYPE_RETURNVALUE                 = 14,
  /* 15 */ MSG_TYPE_STOPPROCESSFORKER           = 15,
  /* 16 */ MSG_TYPE_TAPEMOUNTEDFORMIGRATION     = 16,
  /* 17 */ MSG_TYPE_TAPEMOUNTEDFORRECALL        = 17,
  /* 18 */ MSG_TYPE_TAPEUNMOUNTSTARTED          = 18,
  /* 19 */ MSG_TYPE_TAPEUNMOUNTED               = 19,
  /* 20 */ MSG_TYPE_LABELERROR                  = 20,
  /* 21 */ MSG_TYPE_ACSMOUNTTAPEREADONLY        = 21,
  /* 22 */ MSG_TYPE_ACSMOUNTTAPEREADWRITE       = 22,
  /* 23 */ MSG_TYPE_ACSDISMOUNTTAPE             = 23,
  /* 24 */ MSG_TYPE_ACSFORCEDISMOUNTTAPE        = 24,
  /* 25 */ MSG_TYPE_ADDLOGPARAMS                = 25,
  /* 26 */ MSG_TYPE_DELETELOGPARAMS             = 26,
  /* 27 */ MSG_TYPE_ARCHIVEJOBFROMCTA           = 27,
  /* 28 */ MSG_TYPE_RETRIEVEJOBFROMCTA          = 28
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

} // namespace castor::messages

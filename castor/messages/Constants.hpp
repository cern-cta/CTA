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
  PROTOCOL_TYPE_TAPE
};

enum MsgType {
  MSG_TYPE_EXCEPTION,
  MSG_TYPE_HEARTBEAT,
  MSG_TYPE_MIGRATIONJOBFROMTAPEGATEWAY,
  MSG_TYPE_MIGRATIONJOBFROMWRITETP,
  MSG_TYPE_NBFILESONTAPE,
  MSG_TYPE_RECALLJOBFROMREADTP,
  MSG_TYPE_RECALLJOBFROMTAPEGATEWAY,
  MSG_TYPE_RETURNVALUE,
  MSG_TYPE_TAPEMOUNTEDFORMIGRATION,
  MSG_TYPE_TAPEMOUNTEDFORRECALL,
  MSG_TYPE_TAPEUNMOUNTSTARTED,
  MSG_TYPE_TAPEUNMOUNTED
};

enum ProtocolVersion {
  PROTOCOL_VERSION_1
};

} // namespace messages
} // namespace castor

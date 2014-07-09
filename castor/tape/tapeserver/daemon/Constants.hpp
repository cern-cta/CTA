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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the VDQM server.
 */
const unsigned short TAPE_SERVER_VDQM_LISTENING_PORT = 5070;

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the tpconfig admin command.
 */
const unsigned short TAPE_SERVER_ADMIN_LISTENING_PORT = 5011;

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the label command.
 */
const unsigned short TAPE_SERVER_LABELCMD_LISTENING_PORT = 54321;

/*
 * The port on which ZMQ sockets will bind for internal communication between 
 * motherforker and forked session
 */
const unsigned short TAPE_SERVER_INTERNAL_LISTENING_PORT = 54322;
} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor


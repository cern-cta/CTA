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
const unsigned short TAPESERVER_VDQM_LISTENING_PORT = 5070;

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the tpconfig admin command.
 */
const unsigned short TAPESERVER_ADMIN_LISTENING_PORT = 5011;

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the label command.
 */
const unsigned short TAPESERVER_LABELCMD_LISTENING_PORT = 54321;

/*
 * The port on which ZMQ sockets will bind for internal communication between 
 * forked sessions and the parent tapeserverd process.
 */
const unsigned short TAPESERVER_INTERNAL_LISTENING_PORT = 54322;

/**
 * The compile-time default value for the maximum time in seconds that the
 * data-transfer session can take to get the transfer job from the client.
 */
const time_t TAPESERVER_WAITJOBTIMEOUT_DEFAULT = 60; // 1 minute

/** 
 * The compile-time default value for the maximum time in seconds that the
 * data-transfer session can take to mount a tape.
 */
const time_t TAPESERVER_MOUNTTIMEOUT_DEFAULT = 900; // 15 minutes

/**
 * The compile-time default value for the maximum time in seconds the
 * data-transfer session of tapeserverd can cease to move data blocks.
 */
const time_t TAPESERVER_BLKMOVETIMEOUT_DEFAULT = 300; // 5 minutes

/**
 * The compile-time default value for the number of disk threads in 
 * the thread pool serving disk accesses.
 */
const uint32_t TAPESERVER_NB_DISK_THREAD_DEFAULT = 3;

/**
 * The compile-time default value for the memory buffers exchanged between
 * tape and disk threads.
 */
const size_t TAPESERVER_BUFSZ_DEFAULT = 5 * 1024 * 1024;

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor


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

#include <stdint.h>
#include <stdlib.h>
#include <time.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections and jobs from the VDQM server.
 */
const unsigned short TAPESERVER_JOB_PORT = 5070;

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the tpconfig admin command.
 */
const unsigned short TAPESERVER_ADMIN_PORT = 5011;

/**
 * The TCP/IP port on which the tape server daemon listens for incoming
 * connections from the tape labeling command.
 */
const unsigned short TAPESERVER_LABEL_PORT = 54321;

/**
 * The TCP/IP port on which ZMQ sockets will bind for internal communication
 * between forked sessions and the parent tapeserverd process.
 */
const unsigned short TAPESERVER_INTERNAL_PORT = 54322;

/**
 * The delay in seconds the master process of the tapeserverd daemon should
 * wait before launching another transfer session whilst the corresponding
 * drive is idle.
 */
const unsigned int TAPESERVER_TRANSFERSESSION_TIMER = 10;

/**
 * The compile-time default value for the maximum time in seconds that the
 * data-transfer session can take to get the transfer job from the client.
 */
const time_t TAPESERVER_WAITJOBTIMEOUT = 60; // 1 minute

/** 
 * The compile-time default value for the maximum time in seconds that the
 * data-transfer session can take to mount a tape.
 */
const time_t TAPESERVER_MOUNTTIMEOUT = 900; // 15 minutes

/**
 * The compile-time default value for the maximum time in seconds the
 * data-transfer session of tapeserverd can cease to move data blocks.
 */
const time_t TAPESERVER_BLKMOVETIMEOUT = 1800; // 30 minutes

/**
 * The compile-time default value for the number of disk threads in 
 * the thread pool serving disk accesses.
 */
const uint32_t TAPESERVER_NB_DISK_THREAD = 10;

/**
 * The compile-time default value for the memory buffers exchanged between
 * tape and disk threads.
 */
const size_t TAPESERVER_BUFSZ = 5 * 1024 * 1024;

/**
 * The compile time timeout value for the potentially DB based calls to the client.
 * As those can take time on a contended and for bulk communications, we go above 
 * the default 20 seconds.
 * This value is not configurable.
 */
const int TAPESERVER_DB_TIMEOUT = 60 * 5; // 5 minutes

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

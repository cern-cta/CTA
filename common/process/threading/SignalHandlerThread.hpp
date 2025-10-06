/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#include <signal.h>

#include "common/process/threading/Thread.hpp"

namespace cta::threading {
class SignalHandlerThread : private cta::threading::Thread {
public:
  /**
   * Constructor
   * @param logContext The log context used to
   */
  SignalHandlerThread(const cta::log::LogContext& logContext);

  /**
   * Destructor
   */
  ~SignalHandlerThread() noexcept;

  /**
   * Thread routine: loop waiting for signals to be available, then, sets the
   * signal flag to be handled by the daemonn. The thread loops until SIGTERM or
   * SIGINT is received where the shutdown process will be started.
   */
  void run() override;

protected:
  /**
   * Log
   */
  const cta::log::LogContext m_logContext;

  /**
    * File descriptor to read signals from the blocked signals.
    */
  int m_sigFd;

  /**
   * Shared pointer data structure containig the status of the different signals
   * to be processed by the "client"
   * The role of the handler is to read the received signals by the process and
   * set the correspondent flag
   *
   * The "client" is responsible of consuming the flag and unsetting the bit. 
   *
   * Due to different architectures having differnet values we convert the
   * received signal into an internal representation
   *
   * 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
   * | | | | | | | | | | | | | | | \- SigUsr1        -- Used interanlly in CTA daemons for log rotation.
   * | | | | | | | | | | | | | | \--- SigUsr2        -- Daemon specific functions.
   * | | | | | | | | | | | | | \----- SigInt/SigTerm -- Daemon shutdown 
   * | | | | | | | | | | | | \------- SigHup         -- Reload config paramters.
   * | | | | | | | | | | | \--------- 
   * | | | | | | | | | | \-----------
   * | | | | | | | | | \-------------
   * | | | | | | | | \---------------
   * | | | | | | | \-----------------
   * | | | | | | \-------------------
   * | | | | | \---------------------
   * | | | | \-----------------------
   * | | | \-------------------------
   * | | \---------------------------
   * | \-----------------------------
   * \-------------------------------
   */
  // TODO: DO ew really need the atomic? 
  // TODO: We could benefit from a class handlign the set/unset operations and givin it more semantic meaning. 
  std::atomic<std::shared_ptr<unsigned short int>> signalFlags; 
};

}  // namespace cta::server
   //

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

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include <unistd.h>


namespace cta {
namespace threading {

/**
  * A class allowing forking of a child process, and subsequent follow up
  * of the child process. Status check, killing, return code collection.
  */
class ChildProcess {
public:
  /**
    * Helper functor for child to clean up unneeded parent resources 
    * after forking.
    */
  class Cleanup {
  public:
    virtual void operator() () = 0;
    virtual ~Cleanup() = default;
  };
  /**
    * Exceptions for wrong usage.
    */
  class ProcessStillRunning: public cta::exception::Exception {
  public:
    explicit ProcessStillRunning(const std::string & what = "Process still running"):
    cta::exception::Exception::Exception(what) {}
  };
  
  class ProcessNeverStarted: public cta::exception::Exception {
  public:
    explicit ProcessNeverStarted(const std::string & what = "Process never started"):
    cta::exception::Exception::Exception(what) {}
  };
  
  class ProcessWasKilled: public cta::exception::Exception {
  public:
    explicit ProcessWasKilled(const std::string & what = "Process was killed"):
    cta::exception::Exception::Exception(what) {}
  };
  
  ChildProcess() = default;
  /* Clean up leftover child processes (hopefully not useful) */
  virtual ~ChildProcess() {
    if (!m_finished && m_started) kill();
  };

  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  /** start function, taking as an argument a callback for parent's
    * resources cleanup. A child process can only be fired once. */
  void start(Cleanup & cleanup) ;
  /** Check running status */
  bool running() ;
  /** Wait for completion */
  void wait() ;
  /** collect exit code */
  int exitCode() ;
  /** kill */
  void kill() const;

private:
  pid_t m_pid;
  /** Was the process started? */
  bool m_started = false;
  /** As the process finished? */
  bool m_finished = false;
  /** Did the process exit cleanly? */
  bool m_exited = false;
  /** Was the process killed? */
  bool m_wasKilled = false;
  int m_exitCode = 0;
  /** The function actually being run in the child process. The value returned
    * by run() will be the exit code of the process (if we get that far) */
  virtual int run() = 0;
  void parseStatus(int status);
};
  
} // namespace threading
} // namespace cta

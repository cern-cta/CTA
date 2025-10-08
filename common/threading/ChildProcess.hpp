/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include <unistd.h>


namespace cta::threading {
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
    class ProcessStillRunning : public exception::Exception {
    public:
      explicit ProcessStillRunning(const std::string& what = "Process still running"):
      cta::exception::Exception::Exception(what) {}
    };

    class ProcessNeverStarted : public exception::Exception {
    public:
      explicit ProcessNeverStarted(const std::string& what = "Process never started"):
      cta::exception::Exception::Exception(what) {}
    };

    class ProcessWasKilled : public exception::Exception {
    public:
      explicit ProcessWasKilled(const std::string& what = "Process was killed"):
      cta::exception::Exception::Exception(what) {}
    };

    ChildProcess() : m_started(false), m_finished(false), m_exited(false), m_wasKilled(false), m_exitCode(0) {}
    /* Clean up leftover child processes (hopefully not useful) */
    virtual ~ChildProcess() {
      try {
        if (!m_finished) kill();
      } catch(ProcessNeverStarted&) { }
    };
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
    void kill() ;
  private:
    pid_t m_pid;
    /** Was the process started? */
    bool m_started;
    /** As the process finished? */
    bool m_finished;
    /** Did the process exit cleanly? */
    bool m_exited;
    /** Was the process killed? */
    bool m_wasKilled;
    int m_exitCode;
    /** The function actually being run in the child process. The value returned
     * by run() will be the exit code of the process (if we get that far) */
    virtual int run() = 0;
    void parseStatus(int status);
  };

} // namespace cta::threading

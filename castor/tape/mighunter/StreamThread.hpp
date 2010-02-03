
/******************************************************************************
 *                      StreamThread.hpp
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
 * @(#)$RCSfile: StreamThread.hpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef STREAM_THREAD_HPP
#define STREAM_THREAD_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include <list>
#include "castor/server/BaseDbThread.hpp"

namespace castor   {
namespace tape     {
namespace mighunter{

/**
 * Stream  thread.
 */
class StreamThread : public castor::server::BaseDbThread {

private:  

  /**
   * Datatype used to create lists of StreamPolicyElements.
   */
  typedef std::list<castor::tape::mighunter::StreamPolicyElement>
    StreamPolicyElementList;

  /**
   * The service classes specified on the command-line which the
   * MigHunterDaemon will work on.
   */
  const std::list<std::string> &m_listSvcClass;

  /**
   * The Python dictionary object associated with the stream policy module.
   */
  PyObject *const m_streamPolicyDict;

  /**
   * The indirect exception throw entry point for stream threads that is
   * called by run();
   *
   * @param arg The argument to be passed to the thread.
   */
  void exceptionThrowingRun(void *arg) throw(castor::exception::Exception);

  /**
   * Run the stream policy using the specified policy element as input.
   *
   * Please note that this method does not obtain a lock on the Python
   * interpreter.
   *
   * @param pyFunc The stream-policy Python-function.
   * @param elem   The policy element to be passed to the stream-policy
   *               Python-function.
   */
  int applyStreamPolicy(
    PyObject *const                              pyFunc,
    castor::tape::mighunter::StreamPolicyElement &elem)
    throw(castor::exception::Exception);

public:

  /**
   * Constructor
   *
   * @param svcClassList     The service classes specified on the command-line
   *                         which the MigHunterDaemon will work on.
   * @param streamPolicyDict The Python dictionary object associated with the
   *                         migration policy module.  This parameter must be
   *                         set to NULL if there is no stream-policy
   *                         Python-module.
   */
  StreamThread(
    const std::list<std::string> &svcClassArray,
    PyObject *const              streamPolicyDict) throw();
     
  /**
   * Destructor
   */
  virtual ~StreamThread() throw() {
    // Do nothing
  };

  /**
   * The entry point for stream threads.
   *
   * @param arg The argument to be passed to the thread.
   */
  virtual void run(void *arg);

}; // class StreamThread

} // namespace mighunter
} // namespace tape
} // namespace castor

#endif // STREAM_THREAD_HPP

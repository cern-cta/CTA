
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
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef STREAM_THREAD_HPP
#define STREAM_THREAD_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/exception/InvalidConfiguration.hpp"
#include "castor/server/BaseDbThread.hpp"
#include "castor/tape/mighunter/MigHunterDaemon.hpp"

#include <list>
#include <string>
#include <vector>

namespace castor    {
namespace tape      {
namespace mighunter {

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
   * The inspect.getargspec Python-function.
   */
  PyObject *const m_inspectGetargspecFunc;

  /**
   * The Python dictionary object associated with the stream policy module.
   */
  PyObject *const m_streamPolicyDict;

  /**
   * The migration hunter daemon.
   */
  castor::tape::mighunter::MigHunterDaemon &m_daemon;

  /**
   * The arguments names a stream-policy Python-function must have in order to
   * be considered valid.
   */
  std::vector<std::string> m_expectedStreamPolicyArgNames;

  /**
   * The indirect exception throw entry point for stream threads that is
   * called by run();
   *
   * This method has no throw clause so that it can throw anything.
   *
   * @param arg The argument to be passed to the thread.
   */
  void exceptionThrowingRun(void *const arg);

  /**
   * Run the stream policy against all of the candidate streams of the
   * specified service-class.
   *
   * @param oraSvc       The mighunter service for accessing the database
   *                     back-end.
   * @param svcClassName The name of the service-class to which the
   *                     stream-policy should be applied.
   */
  void applyStreamPolicyToSvcClass(
    castor::tape::mighunter::IMigHunterSvc *const oraSvc,
    const std::string                             &svcClassName)
  throw(castor::exception::InvalidConfiguration, castor::exception::Exception);

  /**
   * Checks whether or not the specified stream-policy Python-function has the
   * correct argument names.
   *
   * This method raises an InvalidConfiguration exception if the specified
   * stream-policy Python-function does not have the correct function argument
   * names.
   *
   * @param streamPolicyName   Input: The name of the stream-policy
   *                           Python-function.
   * @param streamPolicyPyFunc Input: The stream-policy Python-function.
   */
  void checkStreamPolicyArgNames(
   const std::string &streamPolicyName,
    PyObject *const  streamPolicyPyFunc)
    throw(castor::exception::InvalidConfiguration);

  /**
   * Given the list of candidate streams for the specified stream-policy and
   * the number of running streams in each tape-pool for the associated
   * service-class, this method calls the policy to determine the streams
   * accepted and rejected by the policy, and the streams which have no
   * tape-copies attached to them.
   *
   * The streams accepted by the stream-policy should be started and the
   * streams rejected by the policy together with the streams with no
   * tape-copies should be deleted or stopped.
   *
   * @param svcClassId              Input: The database ID of the service-class.
   * @param svcClassName            Input: The name of the service-class.
   * @param svcClassNbDrives        Input: The nbDrives attribute of the
   *                                service-class.
   * @param streamPolicyName        Input: The name of the stream-policy Python
   *                                function to be called.
   * @param streamPolicyPyFunc      Input: The stream-policy Python-function.
   * @param streamsForPolicy        Input: Candidate streams for the
   *                                stream-policy.
   * @param tapePoolsForPolicy      Input/output: The database IDs of the
   *                                current service-class' tape-pools mapped
   *                                to their names and numbers of running
   *                                streams.  This method will increment a
   *                                tape-pool's number of running streams when
   *                                the stream-policy accepts a stream.
   * @param streamsAcceptedByPolicy Output: The IDs of the streams accepted by
   *                                the stream-policy and which should therefore
   *                                be started.
   * @param streamsRejectedByPolicy Output: The IDs of the streams rejected by
   *                                the stream-policy and which should therefore
   *                                be stopped.
   * @param streamsNoTapeCopies     Output: The IDs of the streams with no
   *                                tape-copies which were therefore not passed
   *                                to the stream-policy and which should
   *                                therefore be deleted or stopped.
   */
  void callStreamPolicyForEachStream(
    const u_signed64                svcClassId,
    const std::string               &svcClassName,
    const u_signed64                svcClassNbDrives,
    const std::string               &streamPolicyName,
    PyObject *const                 streamPolicyPyFunc,
    const StreamForStreamPolicyList &streamsForPolicy,
    IdToTapePoolForStreamPolicyMap  &tapePoolsForPolicy,
    std::list<u_signed64>           &streamsAcceptedByPolicy,
    std::list<u_signed64>           &streamsRejectedByPolicy,
    std::list<u_signed64>           &streamsNoTapeCopies)
    throw(castor::exception::Exception);

  /**
   * Calls the stream-policy Python-function for the specified stream and
   * returns the result.
   *
   * This method updates the number of running streams per tape-pool if the
   * specified stream is accepted.
   *
   * @param svcClassId         Input: The database ID of the service-class.
   * @param svcClassName       Input: The name of the service-class.
   * @param svcClassNbDrives   Input: The nbDrives attribute of the service-class.
   * @param streamPolicyName   Input: The name of the stream-policy Python
   *                           function to be called.
   * @param streamPolicyPyFunc Input: The stream-policy Python-function.
   * @param streamForPolicy    Input: The stream for the stream-policy.
   * @param tapePoolsForPolicy Input/output: The database IDs of the current
   *                           service-class' tape-pools mapped to their names
   *                           and numbers of running streams.  This method
   *                           will increment a tape-pool's number of running
   *                           streams when the stream-policy accepts a stream.
   * @return                   True if the specified stream is accepted and
   *                           should therefore be started, else false meaning
   *                           the stream has been rejected and should
   *                           therefore be deleted or stopped.
   */
  bool callStreamPolicyPyFuncForStream(
    const u_signed64               svcClassId,
    const std::string              &svcClassName,
    const u_signed64               svcClassNbDrives,
    const std::string              &streamPolicyName,
    PyObject *const                streamPolicyPyFunc,
    const StreamForStreamPolicy    &streamForPolicy,
    IdToTapePoolForStreamPolicyMap &tapePoolsForPolicy)
    throw(castor::exception::Exception);

public:

  /**
   * Constructor
   *
   * @param svcClassList          The service-classes specified on the 
   *                              command-line which the MigHunterDaemon will
   *                              work on.
   * @param inspectGetargspecFunc The inspect.getargspec Python-function, to be
   *                              used to determine whether or not the
   *                              stream-policy Python-functions have the
   *                              correct signature.
   * @param streamPolicyDict      The Python-dictionary object associated with
   *                              the stream-policy Python-module.
   * @param daemon                The migration-hunter daemon.
   */
  StreamThread(
    const std::list<std::string>             &svcClassArray,
    PyObject                          *const inspectGetargspecFunc,
    PyObject                          *const streamPolicyDict,
    castor::tape::mighunter::MigHunterDaemon &daemon)
    throw(castor::exception::Exception);
     
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

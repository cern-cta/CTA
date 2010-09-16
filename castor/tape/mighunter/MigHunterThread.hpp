/******************************************************************************
 *                      MigHunterThread.hpp
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
 * @(#)$RCSfile: MigHunterThread.hpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef MIGHUNTER_THREAD_HPP
#define MIGHUNTER_THREAD_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/exception/InvalidConfiguration.hpp"
#include "castor/exception/TapeCopyNotFound.hpp"
#include "castor/server/BaseDbThread.hpp"
#include "castor/tape/mighunter/IMigHunterSvc.hpp"
#include "castor/tape/mighunter/MigHunterDaemon.hpp"
#include "castor/tape/mighunter/MigrationPolicyElement.hpp"

#include <list>
#include <map>
#include <set>
#include <stdint.h>
#include <string>

namespace castor    {
namespace tape      {
namespace mighunter {

/**
 * MigHunter thread.
 */
class MigHunterThread :public castor::server::BaseDbThread {

private:

  /**
   * Datatype used to create lists of MigrationPolicyElements.
   */
  typedef std::list<castor::tape::mighunter::MigrationPolicyElement>
    MigrationPolicyElementList;

  /**
   * The service classes specified on the command-line which the
   * MigHunterDaemon will work on.
   */
  const std::list<std::string> &m_listSvcClass;

  /**
   * The minimum amount of data in bytes required for starting a new migration.
   * This value has no effect if there are already running streams for the
   * service class in question.
   */
  const u_signed64 m_migrationDataThreshold;

  /**
   * Specifies whether or not the MigHunterDaemon should apply stream cloning.
   */
  const bool m_doClone;

  /**
   * The inspect.getargspec Python-function.
   */
  PyObject *const m_inspectGetargspecFunc;

  /**
   * The Python dictionary object associated with the migration policy module.
   */
  PyObject *const m_migrationPolicyDict;

  /**
   * The migration hunter daemon.
   */
  castor::tape::mighunter::MigHunterDaemon &m_daemon;

  /**
   * Set to true if the mighunter is to run in test mode.
   */
  const bool m_runInTestMode;

  /**
   * The arguments names a migration-policy Python-function must have in order
   * to be considered valid.
   */
  std::vector<std::string> m_expectedMigrationPolicyArgNames;

  /**
   * The set of the names of the migration-policy Python-functions whose
   * functions-arguments match the specification for this release.
   */
  std::set<std::string> m_namesOfValidPolicyFuncs;

  /**
   * The indirect exception throw entry point for MigHunter threads that is
   * called by run();
   *
   * This method has no exception clause in order to allow it to throw
   * anything.
   *
   * @param arg The argument to be passed to the thread.
   */
  void exceptionThrowingRun(void *const arg);

  /**
   * Get the values of the attributes stored in the name server for the
   * specified candiate.
   */
  void getInfoFromNs(const std::string &svcClassName,
    castor::tape::mighunter::MigrationPolicyElement &elem) const
    throw (castor::exception::Exception);

  /**
   * Throws a TapeCopyNotFound exception if there is a tape-copy in the
   * specifed list of tape-copies to be found which cannot be found in the
   * either the specified map of tape-pools to tape-copy lists or the list of
   * tape-copies to be invalidated.
   *
   * @param tapeCopiesToBeFound The list of tape-copies to be found.
   * @param tapePool2TapeCopies The map of tape-pools to tape-copy lists.
   * @param invalidTapeCopies   The list of tape-copies to be invalidated.
   */
  void checkEachTapeCopyWillBeAttachedOrInvalidated(
    const MigrationPolicyElementList &tapeCopiesToBeFound,
    const std::map<u_signed64, std::list<MigrationPolicyElement> >
      &tapePool2TapeCopies,
    const MigrationPolicyElementList &invalidTapeCopies)
    throw(castor::exception::TapeCopyNotFound);

  /**
   * Returns true if the specifed tape-copy is in the specified map of
   * tape-pools to tape-copy lists.
   *
   * @param tapeCopyToBeFound   The tape-copy to be found.
   * @param tapePool2TapeCopies The map of tape-pools to tape-copy lists.
   */
  bool tapeCopyIsInMapOfTapePool2TapeCopies(
    const MigrationPolicyElement &tapeCopyToBeFound,
    const std::map<u_signed64, MigrationPolicyElementList> &tapePool2TapeCopies)
    throw();

  /**
   * Returns true if the specifed tape-copy is in the specified list of
   * tape-copies.
   *
   * @param tapeCopyToBeFound The tape-copy to be found.
   * @param tapeCopies        The list of tape-copies.
   */
  bool tapeCopyIsInList(const MigrationPolicyElement &tapeCopyToBeFound,
    const MigrationPolicyElementList &tapeCopies) throw();

  /**
   * Checks whether or not the specified migration-policy Python-function has
   * the correct argument names.
   *
   * This method raises an InvalidConfiguration exception if the specified
   * migration-policy Python-function does not have the correct function
   * argument names.
   *
   * @param migrationPolicyName   Input: The name of the stream-policy
   *                              Python-function.
   * @param migrationPolicyPyFunc Input: The stream-policy Python-function.
   */
  void checkMigrationPolicyArgNames(
   const std::string &migrationPolicyName,
    PyObject *const  migrationPolicyPyFunc)
    throw(castor::exception::InvalidConfiguration);

  /**
   * Run the migration policy using the specified policy element as input.
   *
   * @param pyFunc The migration-policy Python-function.
   * @param elem   The policy element to be passed to the migration-policy
   *               Python-function.
   */
  int applyMigrationPolicy(
    PyObject *const                                 pyFunc,
    castor::tape::mighunter::MigrationPolicyElement &elem)
    throw(castor::exception::Exception);
        
public:

  /**
   * Constructor
   *
   * @param svcClassList           The service classes specified on the
   *                               command-line which the MigHunterDaemon will
   *                               work on.
   * @param migrationDataThreshold The minimum amount of data in bytes required
   *                               for starting a new migration.  This value
   *                               has no effect if there are already running
   *                               streams for the service class in question.
   * @param doClone                Specifies whether or not the MigHunterDaemon
   *                               should apply stream cloning.
   * @param inspectGetargspecFunc  The inspect.getargspec Python-function, to
   *                               be used to determine whether or not the
   *                               migration-policy Python-functions have the
   *                               correct signature.
   * @param migrationPolicyDict    The Python dictionary object associated with
   *                               the migration-policy Python-module.
   * @param daemon                 The migration-hunter daemon.
   * @param runInTestMode          Set to true if the mighunter is to run in
   *                               test mode.
   */
  MigHunterThread(
    const std::list<std::string> &svcClassList,
    const uint64_t               migrationDataThreshold,
    const bool                   doClone,
    PyObject              *const inspectGetargspecFunc,
    PyObject              *const migrationPolicyDict,
    MigHunterDaemon              &daemon,
    const bool                   runInTestMode)
    throw(castor::exception::Exception);

  /**
   * Destructor
   */
  virtual ~MigHunterThread() throw() {
    // Do nothing
  };

  /**
   * The entry point for MigHunter threads.
   *
   * @param arg The argument to be passed to the thread.
   */
  virtual void run(void *arg);

}; // MigHunterThread

} // end of namespace mighunter
} // end of namespace tape
} // end of namespace castor

#endif // MIGHUNTER_THREAD_HPP

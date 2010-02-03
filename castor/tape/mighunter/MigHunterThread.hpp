
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

#include <list>
#include <stdint.h>

#include "castor/server/BaseDbThread.hpp"

#include "castor/tape/mighunter/MigrationPolicyElement.hpp"

namespace castor    {
namespace tape    {
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
   * The Python dictionary object associated with the migration policy module.
   */
  PyObject *const m_migrationPolicyDict;

  /**
   * The indirect exception throw entry point for MigHunter threads that is
   * called by run();
   *
   * @param arg The argument to be passed to the thread.
   */
  void exceptionThrowingRun(void *arg) throw(castor::exception::Exception);

  /**
   * Get the values of the attributes stored in the name server for the
   * specified candiate.
   */
  void getInfoFromNs(const std::string &svcClassName,
    castor::tape::mighunter::MigrationPolicyElement &elem) const
    throw (castor::exception::Exception);

  /**
   * Run the migration policy using the specified policy element as input.
   *
   * Please note that this method does not obtain a lock on the Python
   * interpreter.
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
   * @param migrationPolicyDict    The Python dictionary object associated with
   *                               the migration policy module.  This parameter
   *                               must be set to NULL if there is no
   *                               migration-policy Python-module.
   */
  MigHunterThread(
    const std::list<std::string> &svcClassList,
    const uint64_t               migrationDataThreshold,
    const bool                   doClone,
    PyObject *const              migrationPolicyDict) throw();

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

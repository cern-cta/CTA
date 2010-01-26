
/******************************************************************************
 *                      MigHunterDaemon.hpp
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
 * @(#)$RCSfile: MigHunterDaemon.hpp,v $ $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#ifndef MIGHUNTER_DAEMON_HPP
#define MIGHUNTER_DAEMON_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/rtcopy/mighunter/Constants.hpp"
#include "castor/server/BaseDaemon.hpp"

#include <list>
#include <pthread.h>
#include <stdint.h>

namespace castor    {
namespace rtcopy    {
namespace mighunter {

/**
 * MigHunter daemon.
 */
class MigHunterDaemon : public castor::server::BaseDaemon {
private:

  /**
   * DLF message strings.
   */
  static castor::dlf::Message s_dlfMessages[];

  /**
   * The time in seconds between two stream-policy database lookups.
   */
  int m_streamSleepTime;

  /**
   * The time in seconds between two migration-policy database
   */
  int m_migrationSleepTime;

  /**
   * The minimum amount of data in bytes required for starting a new migration.
   * This value has no effect if there are already running streams for the
   * service class in question.
   */
  uint64_t m_migrationDataThreshold;

  /**
   * The service classes specified on the command-line which the
   * MigHunterDaemon will work on.
   */
  std::list<std::string> m_listSvcClass;

  /**
   * Specifies whether or not the MigHunterDaemon should apply stream cloning.
   */
  bool m_doClone;

  /**
   * Logs the start of the daemon.
   */
  void logStart(const int argc, const char *const *const argv) throw();

  /**
   * Exception throwing main() function which basically implements the
   * non-exception throwing main() function except for the initialisation of
   * DLF and the "exception catch and log" logic.
   */
  int exceptionThrowingMain(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if the file corresponding to the specified
   * stream-policy or migration-policy Python-module does not exist in the
   *  CASTOR_POLICIES_DIRECTORY directory.
   *
   * @param moduleName The name of the stream-policy or migration-policy
   *                   Python-module.
   */
  void checkPolicyModuleFileExists(const char *moduleName)
    throw(castor::exception::Exception);


public:

  /**
   * Constructor.
   */
  MigHunterDaemon() throw();

  /**
   * Destructor
   */
  virtual ~MigHunterDaemon() throw() {
    // Do nothing
  };

  /**
   * The main entry function of the mighunter daemon.
   *
   * Please not that this method must be called by the main thread of the
   * application.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  int main(const int argc, char **argv);

  /**
   * Writes the command-line usage message of the daemon to standard out.
   */
  void usage();

  /**
   * Parses the command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(int argc, char* argv[]);

  /**
   * Appends the specified directory to the value of the PYTHONPATH environment
   * variable.
   *
   * @param directory The full pathname of the directory to be appended to the
   *                  value of the PYTHONPATH environment variable.
   */
  void appendDirectoryToPYTHONPATH(const char *const directory) throw();

  /**
   * Imports the specified module into the embedded Python interpreter.
   *
   * @param moduleName The name of the Python module to be imported.
   */
  PyObject *importPythonModule(const char *const moduleName)
    throw(castor::exception::Exception);

}; // class MigHunterDaemon

} // namespace mighunter
} // namespace cleaning
} // namespace castor

#endif // MIGHUNTER_DAEMON_HPP

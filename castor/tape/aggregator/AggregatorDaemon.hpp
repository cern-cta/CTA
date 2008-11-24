/******************************************************************************
 *                 castor/tape/aggregator/AggregatorDaemon.hpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_AGGREGATORDAEMON_HPP
#define CASTOR_TAPE_AGGREAGTOR_AGGREGATORDAEMON_HPP 1

#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"

#include <iostream>


namespace castor {
namespace tape {
namespace aggregator {

/**
 * The aggregator daemon.
 */
class AggregatorDaemon : public castor::server::BaseDaemon {

public:

  /**
   * Constructor.
   *
   * @param daemonName The name of the daemon.
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  AggregatorDaemon(const char *const daemonName, const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Destructor.
   */
  ~AggregatorDaemon() throw();

  /**
   * Parses the command-line arguments and sets the daemon options accordingly.
   *
   * @param helpOption This method sets this parameter to true if the help
   * option was found on the command-line, else this method sets it to false.
   *
   */
  void parseCommandLine(bool &helpRequested)
    throw(castor::exception::Exception);

  /**
   * Logs a start message and then calls the start method of the super class,
   * in other words castor::server::BaseDaemon::start().
   */
  virtual void start() throw (castor::exception::Exception);

  /**
   * Writes the command-line usage message of the daemon onto the specified
   * output stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name of the aggregator daemon.
   */
  static void usage(std::ostream &os) throw();

  /**
   * Returns the port on which the server will listen.
   */
   int getListenPort() throw(castor::exception::InvalidConfigEntry);


private:

  /**
   * DLF message strings.
   */
  static castor::dlf::Message s_dlfMessages[];

  /**
   * Argument count from the executable's entry function: main().
   */
  const int m_argc;

  /**
   * Argument vector from the executable's entry function: main().
   */
  char **m_argv;

  /**
   * Checks if the specified string is a valid unsigned integer.
   *
   * @param str The string to be checked.
   * @returns true if the string is a valid unsigned integer, else false.
   */
  static bool isAValidUInt(char *str) throw();

}; // class AggregatorDaemon

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_AGGREGATORDAEMON_HPP

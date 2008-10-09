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

#include <iostream>


namespace castor {
namespace tape {
namespace aggregator {

/**
 * Default port on which the aggregator listens.
 */
const int AGGREGATOR_PORT = 12345;

  	
/**
 * The aggregator daemon.
 */
class AggregatorDaemon : public castor::server::BaseDaemon {

public:

  /**
   * Constructor.
   *
   * @param daemonName The name of the daemon.
   */
  AggregatorDaemon(const char *const daemonName)
    throw(castor::exception::Exception);

  /**
   * Parses the command-line arguments and sets the daemon options accordingly.
   *
   * @param argc Number of command-line arguments.
   * @param argv Command-line argument values.
   * @param helpOption This method sets this parameter to true if the help
   * option was found on the command-line, else this method sets it to false.
   *
   */
  void parseCommandLine(int argc, char *argv[], bool &helpRequested)
    throw(castor::exception::Exception);

  /**
   * Writes the command-line usage message of the daemon onto the specified
   * output stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name of the aggregator daemon.
   */
  static void usage(std::ostream &os, const char *const programName) throw();

  /**
   * Returns the port on which the server will listen.
   */
  int getListenPort();

private:

  /**
   * DLF message strings.
   */
  static castor::dlf::Message s_dlfMessages[];

  /**
   * The port on which the server will listen.
   */
  int m_listenPort;

}; // class AggregatorDaemon

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_AGGREGATORDAEMON_HPP

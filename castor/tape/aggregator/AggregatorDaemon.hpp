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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_AGGREGATORDAEMON_HPP
#define CASTOR_TAPE_AGGREGATOR_AGGREGATORDAEMON_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/server/BaseDaemon.hpp"
#include "castor/server/BaseThreadPool.hpp"

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
   */
  AggregatorDaemon() throw(castor::exception::Exception);

  /**
   * Destructor.
   */
  ~AggregatorDaemon() throw();

  /**
   * The main entry function of the aggregator daemon.
   */
  int main(const int argc, char **argv);

private:

  /**
   * Logs the start of the daemon.
   */
  void logStart(const int argc, const char *const *const argv) throw();

  /**
   * Data type used to store the results of parsing the command-line.
   */
  struct ParsedCommandLine {
    bool foregroundOptionSet;
    bool helpOptionSet;

    ParsedCommandLine() :
      foregroundOptionSet(false),
      helpOptionSet(false) {
    }
  };

  /**
   * The results of parsing the command-line.
   */
  ParsedCommandLine m_parsedCommandLine;

  /**
   * The VDQM request handler thread pool which should contain as many threads
   * as there are drives per tape server.
   */
  server::BaseThreadPool *m_vdqmRequestHandlerThreadPool;

  /**
   * Parses the command-line arguments and sets the daemon options accordingly.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception);

  /**
   * Writes the command-line usage message of the aggregator daemon onto the
   * specified output stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name to be used in the message.
   */
  void usage(std::ostream &os, const char *const programName) throw();

  /**
   * Creates the VDQM request handler thread pool.
   */
  void createVdqmRequestHandlerPool()
    throw (castor::exception::Exception);

  /**
   * DLF message strings.
   */
  static castor::dlf::Message s_dlfMessages[];

}; // class AggregatorDaemon

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_AGGREGATORDAEMON_HPP

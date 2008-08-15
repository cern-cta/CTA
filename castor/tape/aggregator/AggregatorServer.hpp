/******************************************************************************
 *                 castor/tape/aggregator/AggregatorServer.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_AGGREGATORSERVER_HPP
#define CASTOR_TAPE_AGGREAGTOR_AGGREGATORSERVER_HPP 1

#include "castor/server/BaseDaemon.hpp"


namespace castor {
namespace tape {
namespace aggregator {
  	
/**
 * The aggregator server.
 */
class AggregatorServer : public castor::server::BaseDaemon {

public:

  /**
   * Constructor.
   *
   * Calls the constructor of BaseDaemon with the name of this server and
   * initialises DLF.
   */
  AggregatorServer(const char *const serverName) throw();

  /**
   * Parses the command line and sets the server options accordingly.
   *
   * In case of an error this method writes the appriopriate error messages
   * to both standard error and DLF and then calls exit with a value of 1.
   */
  void parseCommandLine(int argc, char *argv[]) throw();

  /**
   * Initialises the database service.
   *
   * In case of an error this method writes the appriopriate error messages
   * to both standard error and DLF and then calls exit with a value of 1.
   */
  void initDatabaseService();


private:

  /**
   * DLF message strings.
   */
  static castor::dlf::Message s_dlfMessages[];

  /**
   * Prints out the command-line usage message for the server.
   */
  void usage(const char *const programName) throw();

}; // class AggregatorServer

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_AGGREGATORSERVER_HPP
